import os
import re
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, List
from urllib.parse import urljoin

import boto3
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from bs4 import BeautifulSoup


# URL base da data de referência no portal da Casa dos Dados (usei essa base pois o oficial da receita necessita de login agora, isso mudou agora no início de 2026).
BASE_URL = os.getenv(
    "CNPJ_BASE_URL",
    "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/2026-01-11/",
)
REFERENCE_DATE = os.getenv("CNPJ_REFERENCE_DATE", "2026-01-11")
# Lista de arquivos-alvo (ex.: Empresas0, Estabelecimentos0) para permitir recortes menores no case.
DATASET_FILTERS = [
    item.strip()
    for item in os.getenv(
        "CNPJ_DATASETS",
        (
            "Cnaes,Empresas,Estabelecimentos,Motivos,Municipios,"
            "Naturezas,Paises,Qualificacoes,Simples,Socios"
        ),
    ).split(",")
    if item.strip()
]

# Observações:
# - Se existir "<dataset>.zip", ele é selecionado.
# - Se nao existir, busca serie numerada "<dataset>0.zip, <dataset>1.zip...".
# - Com MAX_FILES_PER_FILTER=1, para series numeradas entra somente o arquivo "0".
MAX_FILES_PER_FILTER = int(os.getenv("CNPJ_MAX_FILES_PER_FILTER", "1"))

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "datacorp-bronze")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "cnpj")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")


def _normalize_dataset_name(zip_filename: str) -> str:
    # Remove sufixo numérico (Empresas0 -> empresas) para manter pasta de dataset estável.
    name = os.path.basename(zip_filename).replace(".zip", "")
    name = re.sub(r"\d+$", "", name)
    return name.lower()


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def _ensure_bucket_exists(client, bucket_name: str) -> None:
    # Garante exec da DAG: se o bucket já existe, segue normalmente.
    buckets = [bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])]
    if bucket_name not in buckets:
        client.create_bucket(Bucket=bucket_name)


@dag(
    dag_id="bronze_raw_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cnpj", "bronze", "raw"],
)
def bronze_raw_ingestion():
    @task
    def ensure_bronze_bucket() -> str:
        # Garante que o bucket existe no destino
        s3_client = _build_s3_client()
        _ensure_bucket_exists(s3_client, BRONZE_BUCKET)
        return BRONZE_BUCKET

    @task
    def list_zip_files() -> List[Dict[str, str]]:
        # Faz scraping simples do índice HTML para descobrir os ZIPs disponíveis na data.
        response = requests.get(BASE_URL, timeout=60)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        links = [a.get("href") for a in soup.find_all("a") if a.get("href")]
        zip_links = [href for href in links if href and href.lower().endswith(".zip")]
        zip_link_by_lower_name = {
            os.path.basename(href).lower(): href for href in zip_links
        }

        selected: List[Dict[str, str]] = []
        for dataset_filter in DATASET_FILTERS:
            exact_name = f"{dataset_filter}.zip".lower()
            exact_match = zip_link_by_lower_name.get(exact_name)

            if exact_match:
                matched = [exact_match]
            else:
                numbered_regex = re.compile(
                    rf"^{re.escape(dataset_filter)}(\d+)\.zip$", re.IGNORECASE
                )
                numbered_matches = []
                for href in zip_links:
                    filename = os.path.basename(href.strip())
                    match = numbered_regex.match(filename)
                    if match:
                        numbered_matches.append((int(match.group(1)), href))

                numbered_matches.sort(key=lambda item: item[0])
                matched = [
                    href for _, href in numbered_matches[:MAX_FILES_PER_FILTER]
                ]

            for href in matched:
                filename = os.path.basename(href)
                selected.append(
                    {
                        "zip_name": filename,
                        "zip_url": urljoin(BASE_URL, href),
                        "dataset": _normalize_dataset_name(filename),
                        "reference_date": REFERENCE_DATE,
                    }
                )

        if not selected:
            raise AirflowFailException(
                "No ZIP files matched filters. "
                f"BASE_URL={BASE_URL}, DATASET_FILTERS={DATASET_FILTERS}"
            )

        return selected

    @task
    def download_extract_upload(file_info: Dict[str, str]) -> Dict[str, str]:
        # Particionamento no padrão medalhão: year/month/day.
        reference_date = datetime.strptime(file_info["reference_date"], "%Y-%m-%d")
        year = reference_date.strftime("%Y")
        month = reference_date.strftime("%m")
        day = reference_date.strftime("%d")

        s3_client = _build_s3_client()

        zip_url = file_info["zip_url"]
        zip_name = file_info["zip_name"]
        dataset = file_info["dataset"]

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=True) as temp_zip:
            # Download em chunks para não carregar o ZIP inteiro na memória. (precisei adicionar isso pois obtive alguns erros durante a execução por conta de limitação 2g do docker)
            with requests.get(zip_url, stream=True, timeout=120) as response:
                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        temp_zip.write(chunk)
            temp_zip.flush()

            uploaded_members = 0
            with zipfile.ZipFile(temp_zip.name, "r") as zf:
                members = [m for m in zf.namelist() if not m.endswith("/")]
                if not members:
                    raise AirflowFailException(f"ZIP {zip_name} has no files")

                for member in members:
                    member_info = zf.getinfo(member)
                    if member_info.file_size == 0:
                        continue

                    with zf.open(member, "r") as extracted:
                        key = (
                            f"{BRONZE_PREFIX}/{dataset}/"
                            f"year={year}/month={month}/day={day}/{member}"
                        )
                        # Upload em streaming para evitar OOM em arquivos grandes (ex.: Estabelecimentos0).
                        s3_client.upload_fileobj(extracted, BRONZE_BUCKET, key)
                        uploaded_members += 1

            if uploaded_members == 0:
                raise AirflowFailException(
                    f"ZIP {zip_name} was processed but no non-empty file was uploaded"
                )

        return {
            "zip_name": zip_name,
            "dataset": dataset,
            "uploaded_files": str(uploaded_members),
            "bucket": BRONZE_BUCKET,
        }

    bucket_ready = ensure_bronze_bucket()
    files = list_zip_files()
    bucket_ready >> files
    download_extract_upload.expand(file_info=files)


bronze_raw_ingestion()
