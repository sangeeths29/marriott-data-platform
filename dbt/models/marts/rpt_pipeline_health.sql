-- Operational report: one row per pipeline run, from the Iceberg audit log.
with runs as (
    select * from {{ source('ops', 'pipeline_run_log') }}
),

rejects as (
    select run_id, count(*) as rejected_records
    from {{ source('ops', 'reservation_rejects') }}
    group by run_id
)

select
    r.run_id,
    r.pipeline,
    r.run_date,
    r.status,
    r.watermark,
    cardinality(r.partitions_processed)                     as partitions_processed,
    r.rows_read,
    r.rows_rejected,
    r.rows_after_dedupe,
    r.rows_inserted,
    r.rows_updated,
    r.rows_deleted,
    r.rows_stale_skipped,
    date_diff('second', r.started_at, r.finished_at)        as duration_seconds,
    coalesce(x.rejected_records, 0)                         as rejected_records_logged,
    r.error,
    r.started_at
from runs r
left join rejects x on r.run_id = x.run_id
