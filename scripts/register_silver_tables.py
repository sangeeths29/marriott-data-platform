"""Register the Parquet silver tables (properties, reviews, financials) in the Glue Data Catalog.

Does what the Glue crawler does, but deterministically from the files' own Parquet
schema, so Athena/dbt can query them. Many teams prefer this to crawlers (no schema
guessing, no crawler cost). Also used in hybrid mode, where Spark runs outside Glue.

  DATA_ROOT=s3://<lake-bucket> SILVER_SCHEMA=marriott_dp_dev_silver python scripts/register_silver_tables.py

Idempotent: tables are created or updated; partitions are (re)added.
No-op when DATA_ROOT is a local folder.
"""
import os
import sys

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.types as pat

TABLES = {"properties": [], "reviews": ["source"], "financials": []}  # table -> partition columns


def hive_type(t) -> str:
    if pat.is_string(t) or pat.is_large_string(t):
        return "string"
    if pat.is_boolean(t):
        return "boolean"
    if pat.is_int8(t) or pat.is_int16(t) or pat.is_int32(t):
        return "int"
    if pat.is_int64(t):
        return "bigint"
    if pat.is_float32(t):
        return "float"
    if pat.is_float64(t):
        return "double"
    if pat.is_date(t):
        return "date"
    if pat.is_timestamp(t):
        return "timestamp"
    if pat.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    raise TypeError(f"unmapped Arrow type {t}")


def main() -> int:
    root = os.environ.get("DATA_ROOT", "./data").rstrip("/")
    if not root.startswith("s3://"):
        print("DATA_ROOT is local: nothing to register (DuckDB reads files directly)")
        return 0
    database = os.environ.get("SILVER_SCHEMA", "marriott_dp_dev_silver")
    region = os.environ.get("AWS_REGION", "us-east-1")
    bucket = root[len("s3://"):].split("/", 1)[0]
    s3fs = pafs.S3FileSystem(region=region)
    glue = boto3.client("glue", region_name=region)

    for name, partition_cols in TABLES.items():
        location = f"{root}/silver/{name}/"
        dataset = ds.dataset(f"{bucket}/silver/{name}", filesystem=s3fs, format="parquet",
                             partitioning="hive" if partition_cols else None)
        schema = dataset.schema
        columns = [{"Name": f.name, "Type": hive_type(f.type)} for f in schema if f.name not in partition_cols]
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "PartitionKeys": [{"Name": c, "Type": "string"} for c in partition_cols],
            "StorageDescriptor": {
                "Columns": columns,
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        }
        try:
            glue.update_table(DatabaseName=database, TableInput=table_input)
            action = "updated"
        except glue.exceptions.EntityNotFoundException:
            glue.create_table(DatabaseName=database, TableInput=table_input)
            action = "created"

        added = 0
        if partition_cols:
            values = sorted({tuple(p.split("=", 1)[1] for p in os.path.dirname(f).split("/") if "=" in p)
                             for f in dataset.files})
            sd = dict(table_input["StorageDescriptor"])
            parts = []
            for v in values:
                suffix = "/".join(f"{c}={x}" for c, x in zip(partition_cols, v, strict=True))
                psd = dict(sd, Location=f"{location}{suffix}/")
                parts.append({"Values": list(v), "StorageDescriptor": psd})
            if parts:
                resp = glue.batch_create_partition(DatabaseName=database, TableName=name, PartitionInputList=parts)
                errors = [e for e in resp.get("Errors", [])
                          if e["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException"]
                if errors:
                    raise RuntimeError(errors)
                added = len(parts)
        print(f"{action} {database}.{name}: {len(columns)} columns"
              + (f", {added} partition(s) {partition_cols}" if partition_cols else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
