"""DAG integrity tests: run in CI so a broken DAG never reaches the scheduler."""
import os
from pathlib import Path

import pytest

airflow_models = pytest.importorskip("airflow.models", reason="pip install -r requirements-airflow.txt")

DAGS_DIR = Path(__file__).parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory):
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow_home")))
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags(dagbag):
    assert {"marriott_daily", "crs_simulator"} <= set(dagbag.dag_ids)


def test_daily_structure(dagbag):
    dag = dagbag.dags["marriott_daily"]
    ids = set(dag.task_ids)
    for t in ["reservations.wait_for_crs_extract", "reservations.merge_reservations_cdc",
              "reference_data.is_refresh_day", "reference_data.silver_reference",
              "gold.dbt_build", "publish_freshness_metric"]:
        assert t in ids
    # dbt must wait for both silver jobs (directly, or via the Glue crawler in AWS mode)
    upstream = dag.get_task("gold.dbt_source_freshness").get_flat_relative_ids(upstream=True)
    assert "reservations.merge_reservations_cdc" in upstream
    assert "reference_data.silver_reference" in upstream
    assert dag.max_active_runs == 1 and not dag.catchup


def test_every_task_retries_and_alerts(dagbag):
    for dag_id in ["marriott_daily"]:
        for task in dagbag.dags[dag_id].tasks:
            assert task.owner == "data-platform"
            assert task.on_failure_callback is not None, task.task_id
            # validation steps intentionally don't retry: contract failures need a human
            if not task.task_id.split(".")[-1].startswith(("validate", "publish", "start", "end", "is_")):
                assert task.retries >= 1, task.task_id


def test_sensor_does_not_hog_worker(dagbag):
    sensor = dagbag.dags["marriott_daily"].get_task("reservations.wait_for_crs_extract")
    assert sensor.mode == "reschedule"
