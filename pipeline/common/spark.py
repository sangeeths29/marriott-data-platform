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
    return b.getOrCreate()


def resolve_pii_salt(salt: str | None, ssm_param: str | None) -> str:
    if ssm_param:
        import boto3

        param = boto3.client("ssm").get_parameter(Name=ssm_param, WithDecryption=True)
        return param["Parameter"]["Value"]
    if salt:
        return salt
    log.warning("PII salt not set; using an insecure dev salt")
    return "dev-only-salt"
