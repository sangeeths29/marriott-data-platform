# Runbook: Marriott Data Platform

On-call reference. Alerts arrive on SNS topic `marriott-dp-<env>-alerts`, sent by
Airflow failure/SLA callbacks, EventBridge (Glue) and CloudWatch alarms.

| Alert | Meaning | Section |
|---|---|---|
| `[marriott-dp] FAILED marriott_daily.<task>` | An Airflow task failed after retries | [Triage by task](#triage-by-task) |
| `[marriott-dp] SLA missed` | dbt gold not built within 3h of the run start | [Stale data](#stale-data) |
| `marriott-dp-<env>-glue-job-failed` | A Glue job FAILED / TIMEOUT / ERROR | [Glue job failure](#glue-job-failure) |
| `marriott-dp-<env>-crawler-failed` | Reference silver not registered in the Glue Catalog | [Crawler failure](#crawler-failure) |
| `marriott-dp-<env>-silver-stale` | Silver not refreshed within the SLA (or no metric at all) | [Stale data](#stale-data) |
| Budget forecast > 80% | Spend trending over budget | [Cost](#cost) |

First stop for any reservations issue: the audit log.
```sql
-- Athena (or DuckDB locally: dbt model rpt_pipeline_health)
select run_date, status, watermark, partitions_processed, rows_read, rows_rejected,
       rows_inserted, rows_updated, rows_deleted, rows_stale_skipped, error
from marriott_dp_<env>_ops.pipeline_run_log order by started_at desc limit 20;
```

## Triage by task

| Failing task | Go to |
|---|---|
| `reference_data.ingest_*` | [Upstream source failure](#upstream-source-failure) |
| `*.validate_*` (no retries by design) | [Quarantine](#quarantine) |
| `reservations.wait_for_crs_extract` timed out | [Missing CDC extract](#missing-cdc-extract) |
| `reservations.merge_reservations_cdc` | [CDC merge failure](#cdc-merge-failure) |
| `reference_data.silver_reference` | [Glue job failure](#glue-job-failure) |
| `gold.dbt_source_freshness` | [Stale data](#stale-data) |
| `gold.dbt_build` | [dbt failures](#dbt-failures) |

## Quarantine
A landed file broke its data contract (`pipeline/common/schemas.py`).
```bash
aws s3 ls s3://marriott-dp-<env>-lake-<acct>/quarantine/source=<src>/ingest_date=<date>/
aws s3 cp s3://.../reason_0.json -
```
* **Upstream changed a column:** update the contract and the conform logic, ship it
  via PR, then clear the task in Airflow to re-run it.
* **Truncated / corrupt file:** ask the source to re-deliver (or re-run ingest), then
  clear the task.
Downstream keeps yesterday's data, so consumers see stale-but-correct numbers.

## Missing CDC extract
The reservation system's extract for `{{ ds }}` never landed (the sensor waits 4h).
1. Check the source-side replication (DMS task status / source team).
2. Once it lands, clear `wait_for_crs_extract`; downstream continues.
3. If a day is skipped entirely and arrives later, nothing special is needed: the next
   merge picks up **all** partitions after the watermark, in order.

## CDC merge failure
Look at the latest `FAILED` row in `pipeline_run_log` (column `error`).
* **`reject rate x% exceeds 1.00%`**: the source sent a bad batch. Silver was **not**
  modified and the watermark didn't move. Inspect the rejects:
  ```sql
  select reject_reasons, count(*) from marriott_dp_<env>_ops.reservation_rejects
  where run_id = '<run_id>' group by 1;
  ```
  Then either get a corrected extract (replace the landing partition, re-validate,
  clear the task), or, if the rejects are understood and acceptable, re-run once with
  `--max-reject-rate 0.05` (Glue job parameter override).
* **Iceberg commit conflict:** only one writer is allowed (`max_concurrent_runs=1`,
  `max_active_runs=1`). Check for a manual run that overlapped, then clear the task.
* **Transformation bug that already wrote bad data:** fix the code, deploy, then run
  the job with `--full-refresh`. It empties silver and replays every bronze partition
  from the full load (bronze keeps them all). Then re-run dbt.
* **Need to inspect the pre-incident state:** Iceberg time travel:
  `select * from lake.<ns>_silver.reservations version as of <snapshot_id>`.

## Upstream source failure
* Kaggle 401/403: rotate the `KAGGLE_KEY` credential.
* Overpass 429/504: the public endpoint is overloaded. The task retries with backoff;
  otherwise re-run later.
* SEC 403: `SEC_USER_AGENT` is missing or rate-limited.
Reference data refreshes weekly, so a failed refresh can wait for the next business day.

## Glue job failure
1. Glue console → job run → error message → CloudWatch logs (`/aws-glue/jobs/error`).
2. Common causes: schema drift in bronze, out of memory (raise `glue_workers`), a bad
   deploy (`--extra-py-files` missing).
3. Fix, deploy, clear the Airflow task (both jobs are idempotent).

## Crawler failure
Usually IAM or a partition-type conflict. Check the crawler log, fix, then clear
`crawl_silver`. (The Iceberg tables don't use the crawler; they register themselves.)

## dbt failures
* **Test failure** (unique / not_null / relationships): a data problem. Re-run with
  `dbt test --select <test> --store-failures` and trace the rows back to silver.
* **`value_between` warning on `fct_property_daily.occupancy`**: overbooking above
  100%. This is a business signal, not blocking.
* **`assert_brand_false_positive_rate` warning**: review hotel names and tighten
  `pipeline/common/brands.py`.

## Stale data
1. Did `marriott_daily` run? Check the Airflow UI (scheduler healthy, DAG unpaused).
2. Did it fail? See [Triage by task](#triage-by-task).
3. After fixing, clear the failed run(s). Backfill a range with
   `airflow dags backfill -s 2026-11-05 -e 2026-11-07 marriott_daily`. Every task is
   idempotent, and the CDC merge catches up from its watermark anyway.

## Cost
The Athena workgroup caps each query at 10 GB scanned. Check the dashboard (Athena
bytes, Glue elapsed time). Lower `glue_workers` in tfvars if needed.
