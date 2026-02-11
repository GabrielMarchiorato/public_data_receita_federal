import os
from datetime import datetime
from typing import Dict

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, to_date
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


REFERENCE_DATE = os.getenv("CNPJ_REFERENCE_DATE", "2026-01-11")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "datacorp-bronze")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "cnpj")

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "datacorp-silver")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "cnpj")
SILVER_TABLES = [
    item.strip().lower()
    for item in os.getenv(
        "SILVER_TABLES",
        (
            "cnaes,municipios,motivos,naturezas,paises,qualificacoes,"
            "empresas,estabelecimentos,simples,socios"
        ),
    ).split(",")
    if item.strip()
]
# Deixo configuravel para conseguir rodar subset em debug local (mais leve).

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

HADOOP_AWS_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


def _date_partitions() -> Dict[str, str]:
    # Silver sempre lê o recorte diário que entrou na Bronze.
    dt = datetime.strptime(REFERENCE_DATE, "%Y-%m-%d")
    return {"year": dt.strftime("%Y"), "month": dt.strftime("%m"), "day": dt.strftime("%d")}


def _build_spark() -> SparkSession:
    # Sessao Spark preparada para Delta + acesso S3A no MinIO.
    builder = (
        SparkSession.builder.appName("SilverRefinedDelta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "16")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=HADOOP_AWS_PACKAGES
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def _ensure_bucket_exists(client, bucket_name: str) -> None:
    buckets = [bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])]
    if bucket_name not in buckets:
        client.create_bucket(Bucket=bucket_name)


def _bronze_input_path(dataset: str) -> str:
    parts = _date_partitions()
    return (
        f"s3a://{BRONZE_BUCKET}/{BRONZE_PREFIX}/{dataset}/"
        f"year={parts['year']}/month={parts['month']}/day={parts['day']}/*"
    )


def _silver_output_path(table: str) -> str:
    return f"s3a://{SILVER_BUCKET}/{SILVER_PREFIX}/{table}"


def _assert_not_empty(df: DataFrame, table_name: str) -> None:
    # Validação simples para evitar gravar tabela vazia na Silver.
    if len(df.take(1)) == 0:
        raise AirflowFailException(f"{table_name}: DataFrame vazio na camada Silver")


def _assert_required_not_null(df: DataFrame, table_name: str, column_name: str) -> None:
    # Check de coluna chave obrigatória (sem scan completo desnecessário).
    has_null = df.filter(col(column_name).isNull()).limit(1).count() > 0
    if has_null:
        raise AirflowFailException(
            f"{table_name}: coluna obrigatoria '{column_name}' possui valores nulos"
        )


def _write_delta(df: DataFrame, table: str) -> None:
    # Mantem referencia temporal no dado refinado para rastreabilidade.
    df_to_write = df.withColumn("reference_date", lit(REFERENCE_DATE))
    (
        df_to_write.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(_silver_output_path(table))
    )


def _clean_null_bytes_on_string_columns(df: DataFrame) -> DataFrame:
    # Alguns arquivos vêm com byte nulo em campos texto; limpo só em colunas string.
    cleaned_df = df
    string_columns = [
        field.name for field in cleaned_df.schema.fields if isinstance(field.dataType, StringType)
    ]
    for column_name in string_columns:
        cleaned_df = cleaned_df.withColumn(
            column_name, regexp_replace(col(column_name), "\x00", " ")
        )
    return cleaned_df


def _safe_stop_spark(spark: SparkSession) -> None:
    # Se a JVM morrer por falta de recurso, o stop pode falhar com ConnectionRefused.
    try:
        spark.stop()
    except Exception:
        pass


def _process_single_table(table: str) -> int:
    # Processa uma tabela por task para reduzir pressão de memória no container.
    spark = _build_spark()
    common_csv_options = {
        "header": "false",
        "encoding": "ISO-8859-1",
        "delimiter": ";",
        "quote": '"',
        "escape": "",
    }

    try:
        if table == "cnaes":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("cnaes")
            )
            _assert_not_empty(df, "silver.cnaes")
            _assert_required_not_null(df, "silver.cnaes", "codigo")
            _write_delta(df, "cnaes")
            return -1

        if table == "municipios":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("municipios")
            )
            _assert_not_empty(df, "silver.municipios")
            _assert_required_not_null(df, "silver.municipios", "codigo")
            _write_delta(df, "municipios")
            return -1

        if table == "motivos":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("motivos")
            )
            _assert_not_empty(df, "silver.motivos")
            _assert_required_not_null(df, "silver.motivos", "codigo")
            _write_delta(df, "motivos")
            return -1

        if table == "naturezas":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("naturezas")
            )
            _assert_not_empty(df, "silver.naturezas")
            _assert_required_not_null(df, "silver.naturezas", "codigo")
            _write_delta(df, "naturezas")
            return -1

        if table == "paises":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("paises")
            )
            _assert_not_empty(df, "silver.paises")
            _assert_required_not_null(df, "silver.paises", "codigo")
            _write_delta(df, "paises")
            return -1

        if table == "qualificacoes":
            schema = StructType(
                [
                    StructField("codigo", StringType(), True),
                    StructField("descricao", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("qualificacoes")
            )
            _assert_not_empty(df, "silver.qualificacoes")
            _assert_required_not_null(df, "silver.qualificacoes", "codigo")
            _write_delta(df, "qualificacoes")
            return -1

        if table == "empresas":
            schema = StructType(
                [
                    StructField("cnpj_basico", StringType(), True),
                    StructField("razao_social", StringType(), True),
                    StructField("natureza_juridica", StringType(), True),
                    StructField("qualificacao_responsavel", StringType(), True),
                    StructField("capital_social", StringType(), True),
                    StructField("porte_empresa", IntegerType(), True),
                    StructField("ente_federativo_responsavel", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("empresas")
            )
            df = df.withColumn(
                "capital_social", regexp_replace(col("capital_social"), ",", ".")
            ).withColumn("capital_social", col("capital_social").cast("double"))
            _assert_not_empty(df, "silver.empresas")
            _assert_required_not_null(df, "silver.empresas", "cnpj_basico")
            _write_delta(df, "empresas")
            return -1

        if table == "estabelecimentos":
            schema = StructType(
                [
                    StructField("cnpj_basico", StringType(), True),
                    StructField("cnpj_ordem", StringType(), True),
                    StructField("cnpj_dv", StringType(), True),
                    StructField("identificador_matriz_filial", IntegerType(), True),
                    StructField("nome_fantasia", StringType(), True),
                    StructField("situacao_cadastral", IntegerType(), True),
                    StructField("data_situacao_cadastral", StringType(), True),
                    StructField("motivo_situacao_cadastral", StringType(), True),
                    StructField("nome_cidade_exterior", StringType(), True),
                    StructField("pais", StringType(), True),
                    StructField("data_inicio_atividade", StringType(), True),
                    StructField("cnae_fiscal_principal", StringType(), True),
                    StructField("cnae_fiscal_secundaria", StringType(), True),
                    StructField("tipo_logradouro", StringType(), True),
                    StructField("logradouro", StringType(), True),
                    StructField("numero", StringType(), True),
                    StructField("complemento", StringType(), True),
                    StructField("bairro", StringType(), True),
                    StructField("cep", StringType(), True),
                    StructField("uf", StringType(), True),
                    StructField("municipio", StringType(), True),
                    StructField("ddd_1", StringType(), True),
                    StructField("telefone_1", StringType(), True),
                    StructField("ddd_2", StringType(), True),
                    StructField("telefone_2", StringType(), True),
                    StructField("ddd_fax", StringType(), True),
                    StructField("fax", StringType(), True),
                    StructField("correio_eletronico", StringType(), True),
                    StructField("situacao_especial", StringType(), True),
                    StructField("data_situacao_especial", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("estabelecimentos")
            )
            df = df.withColumn(
                "data_situacao_cadastral", to_date(col("data_situacao_cadastral"), "yyyyMMdd")
            ).withColumn(
                "data_inicio_atividade", to_date(col("data_inicio_atividade"), "yyyyMMdd")
            )
            df = _clean_null_bytes_on_string_columns(df)
            _assert_not_empty(df, "silver.estabelecimentos")
            _assert_required_not_null(df, "silver.estabelecimentos", "cnpj_basico")
            _write_delta(df, "estabelecimentos")
            return -1

        if table == "simples":
            schema = StructType(
                [
                    StructField("cnpj_basico", StringType(), True),
                    StructField("opcao_pelo_simples", StringType(), True),
                    StructField("data_opcao_simples", StringType(), True),
                    StructField("data_exclusao_simples", StringType(), True),
                    StructField("opcao_mei", StringType(), True),
                    StructField("data_opcao_mei", StringType(), True),
                    StructField("data_exclusao_mei", StringType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("simples")
            )
            df = (
                df.withColumn("data_opcao_simples", to_date(col("data_opcao_simples"), "yyyyMMdd"))
                .withColumn("data_exclusao_simples", to_date(col("data_exclusao_simples"), "yyyyMMdd"))
                .withColumn("data_opcao_mei", to_date(col("data_opcao_mei"), "yyyyMMdd"))
                .withColumn("data_exclusao_mei", to_date(col("data_exclusao_mei"), "yyyyMMdd"))
            )
            _assert_not_empty(df, "silver.simples")
            _assert_required_not_null(df, "silver.simples", "cnpj_basico")
            _write_delta(df, "simples")
            return -1

        if table == "socios":
            schema = StructType(
                [
                    StructField("cnpj_basico", StringType(), True),
                    StructField("identificador_socio", IntegerType(), True),
                    StructField("nome_socio_razao_social", StringType(), True),
                    StructField("cpf_cnpj_socio", StringType(), True),
                    StructField("qualificacao_socio", StringType(), True),
                    StructField("data_entrada_sociedade", StringType(), True),
                    StructField("pais", StringType(), True),
                    StructField("representante_legal", StringType(), True),
                    StructField("nome_do_representante", StringType(), True),
                    StructField("qualificacao_representante_legal", StringType(), True),
                    StructField("faixa_etaria", IntegerType(), True),
                ]
            )
            df = spark.read.options(**common_csv_options).schema(schema).csv(
                _bronze_input_path("socios")
            )
            df = df.withColumn("data_entrada_sociedade", to_date(col("data_entrada_sociedade"), "yyyyMMdd"))
            _assert_not_empty(df, "silver.socios")
            _assert_required_not_null(df, "silver.socios", "cnpj_basico")
            _write_delta(df, "socios")
            return -1

        raise AirflowFailException(f"Tabela '{table}' nao suportada para Silver")
    finally:
        _safe_stop_spark(spark)


@dag(
    dag_id="silver_refined_delta",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cnpj", "silver", "delta"],
)
def silver_refined_delta():
    @task
    def ensure_silver_buckets() -> str:
        # Idempotente: se já existir, segue; se não existir, cria.
        s3_client = _build_s3_client()
        _ensure_bucket_exists(s3_client, BRONZE_BUCKET)
        _ensure_bucket_exists(s3_client, SILVER_BUCKET)
        return SILVER_BUCKET

    @task
    def transform_single_table(table: str) -> Dict[str, int]:
        rows = _process_single_table(table)
        return {"table": table, "rows": rows}

    bucket_ready = ensure_silver_buckets()
    # Dynamic task mapping: uma task por tabela da lista SILVER_TABLES.
    transformed = transform_single_table.expand(table=SILVER_TABLES)
    bucket_ready >> transformed


silver_refined_delta()
