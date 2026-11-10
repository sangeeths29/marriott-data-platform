"""Publish HoursSinceSilverRefresh (newest object under silver/) to CloudWatch.

Runs at the end of every nightly build. The CloudWatch alarm fires when the value
exceeds the SLA *or* when no value arrives at all (pipeline never ran).
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.common import storage  # noqa: E402
from pipeline.common.metrics import put_metric  # noqa: E402


def newest_silver_write() -> datetime | None:
    root = storage.join(storage.data_root(), "silver")
    if storage.is_s3(root):
        import boto3

        bucket, key = storage._split(root)
        newest = None
        for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key + "/"):
            for o in page.get("Contents", []):
                newest = max(newest, o["LastModified"]) if newest else o["LastModified"]
        return newest
    files = storage.list_files(root)
    if not files:
        return None
    return datetime.fromtimestamp(max(os.path.getmtime(f) for f in files), tz=timezone.utc)


if __name__ == "__main__":
    newest = newest_silver_write()
    hours = 9999.0 if newest is None else (datetime.now(timezone.utc) - newest).total_seconds() / 3600
    print(f"silver last refreshed {hours:.1f}h ago")
    put_metric("HoursSinceSilverRefresh", round(hours, 2), unit="None")
