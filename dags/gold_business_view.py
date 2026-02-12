import os
from datetime import datetime
from typing import Dict

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit


REFERENCE_DATE = os.getenv("CNPJ_REFERENCE_DATE", "2026-01-11")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "datacorp-silver")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "cnpj")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "datacorp-gold")
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "cnpj")

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

HADOOP_AWS_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def _ensure_bucket_exists(client, bucket_name: str) -> None:
    # Evita quebrar se o bucket ainda não foi criado.
    buckets = [bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])]
    if bucket_name not in buckets:
        client.create_bucket(Bucket=bucket_name)


def _build_spark() -> SparkSession:
    # Sessão padrão para leitura/escrita Delta no MinIO via S3A.
    builder = (
        SparkSession.builder.appName("GoldBusinessView")
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


def _safe_stop_spark(spark: SparkSession) -> None:
    # Em caso de queda da JVM, esse stop pode falhar; ignoro para não mascarar erro real.
    try:
        spark.stop()
    except Exception:
        pass


def _silver_path(table: str) -> str:
    return f"s3a://{SILVER_BUCKET}/{SILVER_PREFIX}/{table}"


def _gold_path(table: str) -> str:
    return f"s3a://{GOLD_BUCKET}/{GOLD_PREFIX}/{table}"


@dag(
    dag_id="gold_business_view",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cnpj", "gold", "delta"],
)
def gold_business_view():
    @task
    def ensure_gold_buckets() -> str:
        # Garante buckets de entrada (Silver) e saída (Gold) antes do processamento.
        s3_client = _build_s3_client()
        _ensure_bucket_exists(s3_client, SILVER_BUCKET)
        _ensure_bucket_exists(s3_client, GOLD_BUCKET)
        return GOLD_BUCKET

    @task
    def build_active_companies_by_cnae_uf() -> Dict[str, int]:
        spark = _build_spark()
        try:
            # Lê tabelas refinadas da Silver.
            df_empresas = spark.read.format("delta").load(_silver_path("empresas"))
            df_estabelecimentos = spark.read.format("delta").load(
                _silver_path("estabelecimentos")
            )
            df_cnaes = spark.read.format("delta").load(_silver_path("cnaes"))

            # Regra de ativo: situacao_cadastral = 2 (receita federal).
            df_ativos = (
                df_estabelecimentos.filter(col("situacao_cadastral") == 2)
                .select(
                    "cnpj_basico",
                    "uf",
                    "cnae_fiscal_principal",
                    "cnpj_ordem",
                    "cnpj_dv",
                )
            )

            df_joined = (
                df_ativos.join(
                    df_empresas.select("cnpj_basico", "porte_empresa"),
                    on="cnpj_basico",
                    how="left",
                )
                .join(
                    df_cnaes.select(
                        col("codigo").alias("cnae_fiscal_principal"),
                        col("descricao").alias("descricao_cnae"),
                    ),
                    on="cnae_fiscal_principal",
                    how="left",
                )
            )

            # Agregado de negocio do case: distribuição por UF e CNAE.
            df_gold = (
                df_joined.groupBy("uf", "cnae_fiscal_principal", "descricao_cnae")
                .agg(countDistinct("cnpj_basico").alias("qtd_empresas_ativas"))
                .withColumn("reference_date", lit(REFERENCE_DATE))
            )

            if len(df_gold.take(1)) == 0:
                raise AirflowFailException(
                    "gold.empresas_ativas_por_cnae_uf: resultado vazio"
                )

            (
                df_gold.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(_gold_path("empresas_ativas_por_cnae_uf"))
            )

            return {"rows_gold": df_gold.count()}
        finally:
            _safe_stop_spark(spark)

    ready = ensure_gold_buckets()
    # Dependência explícita para evitar corrida com bucket ainda inexistente.
    gold_done = build_active_companies_by_cnae_uf()
    ready >> gold_done


gold_business_view()
