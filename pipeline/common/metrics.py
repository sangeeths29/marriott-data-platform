"""Publish custom CloudWatch metrics (no-op when running locally)."""
from __future__ import annotations

import logging
import os

from pipeline.common import storage

NAMESPACE = "MarriottDataPlatform"
log = logging.getLogger("metrics")


def put_metric(name: str, value: float, unit: str = "Count", **dims: str) -> None:
    dims = {"Environment": os.environ.get("ENV", "dev"), **dims}
    if not storage.is_s3(storage.data_root()):
        log.info("metric %s=%s %s (local, not published)", name, value, dims)
        return
    import boto3

    boto3.client("cloudwatch").put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": name, "Value": value, "Unit": unit,
            "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()],
        }],
    )
