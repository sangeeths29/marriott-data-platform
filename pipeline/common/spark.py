"""Shared Spark setup: Iceberg catalog (local Hadoop catalog or AWS Glue Catalog) + secrets."""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from pipeline.common import storage

log = logging.getLogger("spark")

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
CATALOG = "lake"


def namespace(layer: str) -> str:
    """Iceberg namespace == Glue database in AWS, e.g. marriott_dp_dev_silver."""
    return f"{os.environ.get('ICEBERG_NAMESPACE_PREFIX', 'marriott_dp_dev')}_{layer}"


def table(layer: str, name: str) -> str:
    return f"{CATALOG}.{namespace(layer)}.{name}"


# Extra packages a LAPTOP needs to reach S3 + the Glue catalog (Glue jobs ship these built in).
LOCAL_S3_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",  # must match Spark 3.5's bundled Hadoop
])


def runs_locally_against_s3(root: str) -> bool:
    """Hybrid mode: Spark on a laptop/CI runner, data in S3. Set LOCAL_SPARK_S3=1."""
    return storage.is_s3(root) and os.environ.get("LOCAL_SPARK_S3") == "1"


def apply_local_s3(b: SparkSession.Builder, root: str) -> SparkSession.Builder:
    """Let a local Spark read/write s3:// paths with the normal AWS credential chain."""
    if not runs_locally_against_s3(root):
        return b
    region = os.environ.get("AWS_REGION", "us-east-1")
    return (
        b.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
         .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
         .config("spark.sql.iceberg.vectorization.enabled", "false")  # Java 21 laptops, see below
    )


def spark_session(app: str, data_root: str | None = None) -> SparkSession:
    """Plain Spark session (no Iceberg), S3-capable in hybrid mode."""
    root = (data_root or storage.data_root()).rstrip("/")
    return apply_local_s3(SparkSession.builder.appName(app), root).getOrCreate()


def iceberg_session(app: str, data_root: str | None = None) -> SparkSession:
    """Catalog: AWS Glue Catalog when the data root is on S3 (Glue 4.0 job with
    --datalake-formats iceberg), else a local Hadoop catalog. Override with ICEBERG_CATALOG_TYPE.
    Locally, spark-submit needs: --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"""
    root = (data_root or storage.data_root()).rstrip("/")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", f"{root}/iceberg")
    if not storage.is_s3(warehouse):
        # Iceberg records file paths in its metadata; relative paths would only resolve
        # from the directory Spark ran in (dbt/DuckDB run elsewhere).
        warehouse = os.path.abspath(warehouse)
    catalog_type = os.environ.get("ICEBERG_CATALOG_TYPE", "glue" if storage.is_s3(root) else "hadoop")
    b = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if catalog_type == "glue":
        b = (b.config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
              .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"))
    else:
        b = (b.config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
              # Iceberg's vectorized (Arrow) reader can crash the JVM on Java 21 laptops; Glue
              # (Java 8/17) keeps it on. Small local data doesn't need it.
              .config("spark.sql.iceberg.vectorization.enabled", "false"))
    return apply_local_s3(b, root).getOrCreate()


def resolve_pii_salt(salt: str | None, ssm_param: str | None) -> str:
    if ssm_param:
        import boto3

        param = boto3.client("ssm").get_parameter(Name=ssm_param, WithDecryption=True)
        return param["Parameter"]["Value"]
    if salt:
        return salt
    log.warning("PII salt not set; using an insecure dev salt")
    return "dev-only-salt"
