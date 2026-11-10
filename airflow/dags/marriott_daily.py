"""
### marriott_daily

Nightly batch for the Marriott hospitality data platform.

| Group | What | Cadence |
|---|---|---|
| `reference_data` | Kaggle, OpenStreetMap, SEC → landing → bronze → silver (Spark) | Mondays / first run |
| `reservations` | Wait for the reservation system's CDC extract → validate → incremental Iceberg MERGE | Daily |
| `gold` | dbt source freshness → dbt build (models + tests) | Daily |
| `observability` | Silver freshness metric → CloudWatch | Daily |

* Runs for `{{ ds }}`. Every step is idempotent, so clear-and-rerun or
  `airflow dags backfill -s <start> -e <end> marriott_daily` is safe.
* `PIPELINE_ENV=local` runs Spark/dbt in the worker (Docker demo);
  `PIPELINE_ENV=aws` triggers the Glue jobs/crawler and runs dbt against Athena.
* Failures and SLA misses alert via SNS (`ALERT_SNS_TOPIC_ARN`), or the task log locally.
* Runbook: `ops/runbook.md`.

Reservation data comes from a **simulated** reservation system (`pipeline/simulator/crs.py`).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

ENV = os.environ.get("PIPELINE_ENV", "local")  # local | aws
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/marriott")
DBT = os.environ.get("DBT_BIN", "dbt")
DBT_TARGET = "local" if ENV == "local" else "dev"
REFERENCE_SOURCES = ["datafiniti_reviews", "europe_reviews", "osm_properties", "sec_financials"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"

GOLD_DATASET = Dataset("s3://marriott-dp/gold/marts")
SILVER_RESERVATIONS = Dataset("s3://marriott-dp/silver/reservations")


# ------------------------------------------------------------------ alerting
def _publish_alert(subject: str, message: str) -> None:
    topic = os.environ.get("ALERT_SNS_TOPIC_ARN")
    if not topic:
        log.error("ALERT (no SNS topic configured): %s | %s", subject, message)
        return
    import boto3

    boto3.client("sns").publish(TopicArn=topic, Subject=subject[:100], Message=message)


def notify_failure(context) -> None:
    ti = context["task_instance"]
    _publish_alert(
        f"[marriott-dp] FAILED {ti.dag_id}.{ti.task_id}",
        f"Run {context['run_id']} for {context['ds']} failed after {ti.try_number} tries.\n"
        f"Log: {ti.log_url}\nRunbook: ops/runbook.md",
    )


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    _publish_alert(f"[marriott-dp] SLA missed in {dag.dag_id}", f"Tasks: {task_list}\nBlocking: {blocking_task_list}")


# ------------------------------------------------------------------ helpers
def is_reference_refresh_day(ds: str, prev_start_date_success=None, **_) -> bool:
    """Reference data is re-published rarely: refresh on Mondays, or when nothing has succeeded yet."""
    return datetime.fromisoformat(ds).weekday() == 0 or prev_start_date_success is None


def crs_extract_landed(ds: str, **_) -> bool:
    import sys

    sys.path.insert(0, PROJECT_DIR)
    from pipeline.common import storage

    return bool(storage.list_files(storage.partition_path("landing", "crs_reservations", ds)))


def bash(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f'set -euo pipefail; cd "{PROJECT_DIR}"; {command}',
                        append_env=True, **kwargs)


def spark_job(task_id: str, script: str, iceberg: bool, glue_job_env: str, **kwargs):
    if ENV == "aws":
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

        return GlueJobOperator(task_id=task_id, job_name=os.environ[glue_job_env],
                               script_args={"--date": "{{ ds }}"}, wait_for_completion=True,
                               verbose=False, **kwargs)
    packages = f"--packages {ICEBERG_PACKAGE} " if iceberg else ""
    return bash(task_id, f"make package >/dev/null && spark-submit {packages}--py-files build/pipeline.zip "
                         f'{script} --date {{{{ ds }}}} --data-root "$DATA_ROOT"', **kwargs)


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="marriott_daily",
    description="Nightly hospitality data platform batch",
    doc_md=__doc__,
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=notify_sla_miss,
    tags=["marriott-dp", "batch", ENV],
) as dag:
    start = EmptyOperator(task_id="start")

    # -------------------------------------------------------- reference data (weekly)
    with TaskGroup("reference_data") as reference_data:
        gate = ShortCircuitOperator(
            task_id="is_refresh_day",
            python_callable=is_reference_refresh_day,
            ignore_downstream_trigger_rules=False,  # only skip this group, not the whole DAG
        )
        landed = []
        for src in REFERENCE_SOURCES:
            ingest = bash(f"ingest_{src}", f"python -m pipeline.ingestion.ingest --source {src} --date {{{{ ds }}}}",
                          retries=3, pool="external_apis")
            validate = bash(f"validate_{src}",
                            f"python -m pipeline.ingestion.validate_and_load --source {src} --date {{{{ ds }}}}",
                            retries=0)  # a contract failure needs a human, not a retry
            gate >> ingest >> validate
            landed.append(validate)
        silver_reference = spark_job("silver_reference", "pipeline/jobs/bronze_to_silver.py", iceberg=False,
                                     glue_job_env="GLUE_JOB_REFERENCE")
        landed >> silver_reference
        if ENV == "aws":
            from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

            silver_reference >> GlueCrawlerOperator(
                task_id="crawl_silver", config={"Name": os.environ["GLUE_CRAWLER"]})

    # -------------------------------------------------------- reservations (daily CDC)
    with TaskGroup("reservations") as reservations:
        wait_extract = PythonSensor(
            task_id="wait_for_crs_extract",
            python_callable=crs_extract_landed,
            mode="reschedule",           # frees the worker slot while waiting
            poke_interval=10 * 60,
            timeout=4 * 60 * 60,         # extract normally lands ~01:00; give up after 4h
            soft_fail=False,
        )
        validate_crs = bash("validate_crs_extract",
                            "python -m pipeline.ingestion.validate_and_load --source crs_reservations "
                            "--date {{ ds }}", retries=0)
        merge = spark_job("merge_reservations_cdc", "pipeline/jobs/reservations_cdc_merge.py", iceberg=True,
                          glue_job_env="GLUE_JOB_RESERVATIONS", outlets=[SILVER_RESERVATIONS])
        wait_extract >> validate_crs >> merge

    # -------------------------------------------------------- gold (dbt)
    with TaskGroup("gold") as gold:
        freshness = bash("dbt_source_freshness",
                         f"cd dbt && {DBT} source freshness --profiles-dir . --target {DBT_TARGET} "
                         f"--select source:silver_iceberg",
                         trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        dbt_build = bash("dbt_build", f"cd dbt && {DBT} build --profiles-dir . --target {DBT_TARGET}",
                         retries=1, outlets=[GOLD_DATASET], sla=timedelta(hours=3))
        freshness >> dbt_build

    # -------------------------------------------------------- observability
    publish_freshness = bash("publish_freshness_metric", "python scripts/check_freshness.py",
                             trigger_rule=TriggerRule.ALL_DONE, retries=0)  # report even after failures
    end = EmptyOperator(task_id="end")

    start >> [reference_data, reservations]
    [reference_data, reservations] >> gold >> publish_freshness >> end
