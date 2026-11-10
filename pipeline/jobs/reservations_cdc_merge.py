"""Incremental CDC merge: bronze crs_reservations -> Iceberg silver (PySpark / AWS Glue).

Source: the simulated reservation system's DMS-style feed (full load + daily CDC,
Op = I/U/D, monotonically increasing _cdc_seq). See pipeline/simulator/crs.py.

Each run:
  1. Reads the high-water mark (last successfully merged ingest_date) from the
     ops.pipeline_run_log Iceberg table and picks up EVERY bronze partition after it,
     up to --date. Missed days are caught up automatically; re-running is a no-op.
  2. Data-quality gate: rule violations go to ops.reservation_rejects with a reason.
     If more than --max-reject-rate of the batch is rejected, the run fails before
     touching silver.
  3. Keeps only the latest change per reservation in the batch (by _cdc_seq).
  4. MERGE INTO silver.reservations with a sequence guard: a change is applied only
     if its _cdc_seq is newer than the stored row. Late or re-delivered events can
     never overwrite newer data. Hard deletes (Op = D) remove the row.
  5. Rebuilds silver.reservation_nights (one row per occupied room-night) from the
     current reservation state; it feeds occupancy / ADR / RevPAR in dbt.
  6. Appends a run record (counts, watermark, status, duration) to the run log,
     including on failure.

  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
      --py-files build/pipeline.zip pipeline/jobs/reservations_cdc_merge.py --date 2026-11-03

  --full-refresh  empties silver and replays every partition from the full load
                  (use after fixing a transformation bug).
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common import storage
from pipeline.common.pii import hash_identifier
from pipeline.common.spark import iceberg_session, namespace, resolve_pii_salt, table

log = logging.getLogger("reservations_cdc_merge")

PIPELINE = "reservations_cdc"
SOURCE = "crs_reservations"
TARGET = NIGHTS = RUN_LOG = REJECTS = ""  # set by configure_tables()


def configure_tables(namespace_prefix: str | None = None) -> None:
    """Resolve fully-qualified Iceberg table names (namespace == Glue database per environment)."""
    global TARGET, NIGHTS, RUN_LOG, REJECTS
    if namespace_prefix:
        os.environ["ICEBERG_NAMESPACE_PREFIX"] = namespace_prefix
    TARGET = table("silver", "reservations")
    NIGHTS = table("silver", "reservation_nights")
    RUN_LOG = table("ops", "pipeline_run_log")
    REJECTS = table("ops", "reservation_rejects")


configure_tables()
OCCUPYING_STATUSES = ["BOOKED", "MODIFIED", "CHECKED_IN", "CHECKED_OUT"]
CURRENCIES = ["USD", "GBP", "EUR"]

RESERVATION_COLUMNS = """
    reservation_id string, confirmation_number string, property_id string, guest_key string,
    loyalty_tier string, booking_channel string, rate_plan string, room_type string,
    booking_ts timestamp, arrival_date date, departure_date date, nights int, rooms int, adults int,
    currency string, adr_local decimal(12,2), total_amount_local decimal(14,2), status string,
    cancel_ts timestamp, updated_at timestamp, source_system string,
    _cdc_op string, _cdc_seq bigint, _ingest_date string, _merged_at timestamp
"""

def dq_rules() -> dict:
    """Data-quality rules: name -> Column that is TRUE when the row VIOLATES the rule.
    (A function because Spark Columns can't be built before a session exists.)"""
    return {
        "null_reservation_id": F.col("reservation_id").isNull(),
        "invalid_op": ~F.col("Op").isin("I", "U", "D") | F.col("Op").isNull(),
        "null_cdc_seq": F.col("_cdc_seq").isNull(),
        "invalid_stay_dates": F.col("arrival_date").isNull() | (F.col("departure_date") <= F.col("arrival_date")),
        "invalid_rooms": F.col("rooms").isNull() | (F.col("rooms") <= 0),
        "negative_rate": F.col("adr_local") < 0,
        "unknown_currency": ~F.col("currency").isin(*CURRENCIES),
    }


# ---------------------------------------------------------------- setup
def ensure_tables(spark: SparkSession) -> None:
    for layer in ("silver", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace(layer)}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET} ({RESERVATION_COLUMNS})
        USING iceberg
        PARTITIONED BY (months(arrival_date))
        TBLPROPERTIES ('format-version' = '2', 'write.metadata.delete-after-commit.enabled' = 'true')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RUN_LOG} (
            run_id string, pipeline string, run_date string, status string, watermark string,
            partitions_processed array<string>, rows_read bigint, rows_rejected bigint, rows_after_dedupe bigint,
            rows_inserted bigint, rows_updated bigint, rows_deleted bigint, rows_stale_skipped bigint,
            started_at timestamp, finished_at timestamp, error string)
        USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTS} (
            run_id string, reject_reasons string, reservation_id string, op string, cdc_seq string,
            ingest_date string, raw_record string, rejected_at timestamp)
        USING iceberg
        PARTITIONED BY (ingest_date)
    """)


# ---------------------------------------------------------------- watermark + batch
def last_watermark(spark: SparkSession) -> str | None:
    row = spark.sql(
        f"SELECT max(watermark) AS w FROM {RUN_LOG} WHERE pipeline = '{PIPELINE}' AND status = 'SUCCEEDED'"
    ).first()
    return row["w"] if row else None


def pending_partitions(root: str, after: str | None, upto: str) -> list[str]:
    prefix = storage.join(root, "bronze", f"source={SOURCE}")
    dates = {m.group(1) for f in storage.list_files(prefix)
             if f.endswith(".parquet") and (m := re.search(r"ingest_date=(\d{4}-\d{2}-\d{2})", f))}
    return sorted(d for d in dates if (after is None or d > after) and d <= upto)


def read_batch(spark: SparkSession, root: str, partitions: list[str]) -> DataFrame:
    paths = [storage.join(root, "bronze", f"source={SOURCE}", f"ingest_date={d}") for d in partitions]
    raw = spark.read.option("mergeSchema", "true").parquet(*paths)  # bronze: all strings
    return raw.select(
        "*",
        F.col("_cdc_seq").cast("bigint").alias("_seq"),
        F.to_date("arrival_date").alias("_arrival"),
        F.to_date("departure_date").alias("_departure"),
        F.col("rooms").cast("int").alias("_rooms"),
        F.col("adr_local").cast("decimal(12,2)").alias("_adr"),
    )


def apply_dq(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (valid, rejected). Rules are evaluated on typed columns; the original record is
    kept as JSON first so rejects show exactly what the source sent."""
    source_cols = [c for c in df.columns if not c.startswith("_") or c.startswith("_cdc")]
    df = df.withColumn("_raw_record", F.to_json(F.struct(*source_cols)))
    typed = (df.withColumn("_cdc_seq", F.col("_seq")).withColumn("arrival_date", F.col("_arrival"))
               .withColumn("departure_date", F.col("_departure")).withColumn("rooms", F.col("_rooms"))
               .withColumn("adr_local", F.col("_adr")))
    reasons = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in dq_rules().items()])
    flagged = typed.withColumn("_reject_reasons", reasons)
    return flagged.where(F.col("_reject_reasons") == ""), flagged.where(F.col("_reject_reasons") != "")


def latest_per_key(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("reservation_id").orderBy(F.col("_cdc_seq").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def conform(df: DataFrame, salt: str) -> DataFrame:
    hash_udf = F.udf(lambda v: hash_identifier(v, salt), T.StringType())
    return df.select(
        "reservation_id", "confirmation_number", "property_id",
        hash_udf("guest_email").alias("guest_key"),  # raw email never reaches silver
        "loyalty_tier", "booking_channel", "rate_plan", "room_type",
        F.to_timestamp("booking_ts").alias("booking_ts"),
        "arrival_date", "departure_date",
        F.datediff("departure_date", "arrival_date").alias("nights"),
        "rooms", F.col("adults").cast("int").alias("adults"), "currency",
        "adr_local", F.col("total_amount_local").cast("decimal(14,2)").alias("total_amount_local"),
        "status", F.to_timestamp("cancel_ts").alias("cancel_ts"), F.to_timestamp("updated_at").alias("updated_at"),
        "source_system",
        F.col("Op").alias("_cdc_op"), "_cdc_seq", "_ingest_date",
        F.current_timestamp().alias("_merged_at"),
    )


# ---------------------------------------------------------------- merge
def merge_counts(spark: SparkSession, batch: DataFrame) -> dict:
    existing = spark.table(TARGET).select("reservation_id", F.col("_cdc_seq").alias("_t_seq"))
    j = batch.select("reservation_id", "_cdc_op", "_cdc_seq").join(existing, "reservation_id", "left")
    newer = F.col("_cdc_seq") > F.col("_t_seq")
    row = j.agg(
        F.sum(F.when(F.col("_t_seq").isNull() & (F.col("_cdc_op") != "D"), 1).otherwise(0)).alias("ins"),
        F.sum(F.when(F.col("_t_seq").isNotNull() & newer & (F.col("_cdc_op") != "D"), 1).otherwise(0)).alias("upd"),
        F.sum(F.when(F.col("_t_seq").isNotNull() & newer & (F.col("_cdc_op") == "D"), 1).otherwise(0)).alias("del"),
        F.sum(F.when(F.col("_t_seq").isNotNull() & ~newer, 1).otherwise(0)).alias("stale"),
    ).first()
    return {k: int(row[k] or 0) for k in ("ins", "upd", "del", "stale")}


def merge(spark: SparkSession, batch: DataFrame) -> None:
    batch.createOrReplaceTempView("cdc_batch")
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING cdc_batch s
        ON t.reservation_id = s.reservation_id
        WHEN MATCHED AND s._cdc_seq > t._cdc_seq AND s._cdc_op = 'D' THEN DELETE
        WHEN MATCHED AND s._cdc_seq > t._cdc_seq THEN UPDATE SET *
        WHEN NOT MATCHED AND s._cdc_op != 'D' THEN INSERT *
    """)


def rebuild_nights(spark: SparkSession) -> int:
    """One row per occupied room-night. Full atomic replace (Iceberg snapshot): fine at this
    scale; at large scale you'd overwrite only the stay-date partitions touched by the batch."""
    res = spark.table(TARGET).where(F.col("status").isin(*OCCUPYING_STATUSES))
    nights = res.select(
        "reservation_id", "property_id", "booking_channel", "rate_plan", "room_type", "loyalty_tier",
        "currency", "status",
        F.explode(F.sequence("arrival_date", F.date_sub("departure_date", 1))).alias("stay_date"),
        F.col("rooms").alias("room_nights"),
        (F.col("adr_local") * F.col("rooms")).cast("decimal(14,2)").alias("room_revenue_local"),
    )
    nights.writeTo(NIGHTS).using("iceberg").partitionedBy(F.months("stay_date")).createOrReplace()
    return spark.table(NIGHTS).count()


def write_run_log(spark: SparkSession, record: dict) -> None:
    schema = spark.table(RUN_LOG).schema
    spark.createDataFrame([record], schema=schema).writeTo(RUN_LOG).append()


# ---------------------------------------------------------------- main
def run(spark: SparkSession, root: str, run_date: str, salt: str, max_reject_rate: float,
        full_refresh: bool = False) -> dict:
    ensure_tables(spark)
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    record = {"run_id": run_id, "pipeline": PIPELINE, "run_date": run_date, "started_at": started,
              "partitions_processed": [], "error": None}
    try:
        if full_refresh:
            log.warning("FULL REFRESH: emptying %s and replaying all partitions", TARGET)
            spark.sql(f"DELETE FROM {TARGET}")
        watermark = None if full_refresh else last_watermark(spark)
        partitions = pending_partitions(root, watermark, run_date)
        record["partitions_processed"] = partitions
        if not partitions:
            log.info("nothing to do: watermark=%s run_date=%s", watermark, run_date)
            record.update(status="SKIPPED", watermark=watermark, finished_at=datetime.now(timezone.utc))
            write_run_log(spark, record)
            return record

        raw = read_batch(spark, root, partitions).cache()
        rows_read = raw.count()
        valid, rejected = apply_dq(raw)
        rows_rejected = rejected.count()
        if rows_rejected:
            (rejected.select(
                F.lit(run_id).alias("run_id"), F.col("_reject_reasons").alias("reject_reasons"),
                "reservation_id", F.col("Op").alias("op"), F.col("_cdc_seq").cast("string").alias("cdc_seq"),
                F.col("_ingest_date").alias("ingest_date"),
                F.col("_raw_record").alias("raw_record"),
                F.current_timestamp().alias("rejected_at"))
             .writeTo(REJECTS).append())
            log.warning("%s rows rejected (see %s)", rows_rejected, REJECTS)
        record.update(rows_read=rows_read, rows_rejected=rows_rejected)  # kept even if the run fails below
        if rows_read and rows_rejected / rows_read > max_reject_rate:
            raise ValueError(f"reject rate {rows_rejected / rows_read:.2%} exceeds {max_reject_rate:.2%}")

        batch = conform(latest_per_key(valid), salt).cache()
        counts = merge_counts(spark, batch)
        merge(spark, batch)
        room_nights = rebuild_nights(spark)
        record.update(
            status="SUCCEEDED", watermark=partitions[-1], rows_read=rows_read, rows_rejected=rows_rejected,
            rows_after_dedupe=batch.count(), rows_inserted=counts["ins"], rows_updated=counts["upd"],
            rows_deleted=counts["del"], rows_stale_skipped=counts["stale"],
            finished_at=datetime.now(timezone.utc))
        log.info("merged %s: %s | room-night rows: %s", partitions, counts, room_nights)
        write_run_log(spark, record)
        return record
    except Exception as e:
        record.update(status="FAILED", watermark=None, error=repr(e)[:2000], finished_at=datetime.now(timezone.utc))
        write_run_log(spark, record)
        raise


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="process bronze partitions up to this ingest_date")
    p.add_argument("--data-root", default=os.environ.get("DATA_ROOT", "./data"))
    p.add_argument("--pii-salt", default=os.environ.get("PII_SALT"))
    p.add_argument("--pii-salt-ssm-param", default=None)
    p.add_argument("--max-reject-rate", type=float, default=0.01)
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--iceberg-namespace-prefix", default=None, help="e.g. marriott_dp_prod (AWS)")
    a, _ = p.parse_known_args()
    configure_tables(a.iceberg_namespace_prefix)
    root = a.data_root.rstrip("/")
    spark = iceberg_session("marriott-reservations-cdc-merge", root)
    run(spark, root, a.date, resolve_pii_salt(a.pii_salt, a.pii_salt_ssm_param), a.max_reject_rate,
        a.full_refresh)
    spark.stop()


if __name__ == "__main__":
    main()
