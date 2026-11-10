"""Tiny storage abstraction so the same code runs against a local folder or S3.

DATA_ROOT examples:
  ./data                         (local dev, default)
  s3://marriott-dp-dev-lake      (AWS)

Layout under DATA_ROOT:
  landing/source=<src>/ingest_date=<YYYY-MM-DD>/<file>
  bronze/source=<src>/ingest_date=<YYYY-MM-DD>/part.parquet
  quarantine/source=<src>/ingest_date=<YYYY-MM-DD>/<file> + reason.json
  silver/<table>/...
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "./data").rstrip("/")


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def join(*parts: str) -> str:
    return "/".join(p.strip("/") if i else p.rstrip("/") for i, p in enumerate(parts))


def partition_path(zone: str, source: str, ingest_date: str) -> str:
    return join(data_root(), zone, f"source={source}", f"ingest_date={ingest_date}")


def _s3():
    import boto3  # imported lazily so local dev doesn't need AWS creds

    return boto3.client("s3")


def _split(s3_uri: str) -> tuple[str, str]:
    bucket, _, key = s3_uri[len("s3://"):].partition("/")
    return bucket, key


def put_file(local_file: str | Path, dest: str) -> None:
    if is_s3(dest):
        bucket, key = _split(dest)
        _s3().upload_file(str(local_file), bucket, key)
    else:
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_file, dest)


def download(path: str, tmp_dir: Path) -> str:
    """Return a local path for `path`, downloading from S3 into tmp_dir if needed."""
    if not is_s3(path):
        return path
    bucket, key = _split(path)
    local = Path(tmp_dir) / key.replace("/", "__")
    _s3().download_file(bucket, key, str(local))
    return str(local)


def write_json(obj: dict, dest: str) -> None:
    body = json.dumps(obj, indent=2, default=str)
    if is_s3(dest):
        bucket, key = _split(dest)
        _s3().put_object(Bucket=bucket, Key=key, Body=body.encode())
    else:
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_text(body)


def list_files(prefix: str) -> list[str]:
    if is_s3(prefix):
        bucket, key = _split(prefix)
        paginator = _s3().get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/"):
            out += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])]
        return out
    p = Path(prefix)
    return [str(f) for f in sorted(p.rglob("*")) if f.is_file()] if p.exists() else []


def delete_prefix(prefix: str) -> None:
    """Used to make partition writes idempotent (overwrite on re-run)."""
    if is_s3(prefix):
        bucket, key = _split(prefix)
        files = list_files(prefix)
        for i in range(0, len(files), 1000):
            batch = [{"Key": _split(f)[1]} for f in files[i:i + 1000]]
            _s3().delete_objects(Bucket=bucket, Delete={"Objects": batch})
    elif Path(prefix).exists():
        shutil.rmtree(prefix)
