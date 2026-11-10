"""
### crs_simulator (local demo only)

Plays the role of the **upstream reservation system**: each night it publishes that
day's CDC extract (AWS DMS format) into the landing zone, which `marriott_daily`
then waits for. In a real deployment this DAG doesn't exist: the extract would
come from AWS DMS replicating the operational database.

One-time setup (after the first reference refresh has built silver/properties):
`python -m pipeline.simulator.crs init --start <date> --days 30`
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/marriott")

with DAG(
    dag_id="crs_simulator",
    description="SIMULATED upstream reservation system: daily CDC extract",
    doc_md=__doc__,
    schedule="0 1 * * *",  # extract lands ~2h before marriott_daily
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "source-system-simulation", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["simulation", "local-only"],
    is_paused_upon_creation=os.environ.get("PIPELINE_ENV", "local") != "local",
) as dag:
    BashOperator(
        task_id="publish_cdc_extract",
        bash_command=f'cd "{PROJECT_DIR}" && python -m pipeline.simulator.crs advance --date {{{{ ds }}}}',
        append_env=True,
    )
