"""Integration tests for the incremental CDC merge (local Spark + Iceberg).

Hand-built batches cover every case the merge must get right: inserts, same-batch
duplicates, out-of-order (stale) events, hard deletes, DQ rejects, watermark
catch-up, idempotent re-runs, and a failed run that must not touch silver.

Slow-ish (starts Spark, downloads the Iceberg jar on first run). Skipped if pyspark
or Java isn't available:  pytest -m spark
"""
import os
import shutil

import pandas as pd
import pytest

pytest.importorskip("pyspark")
if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
    pytest.skip("Java not available", allow_module_level=True)

pytestmark = pytest.mark.spark

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
DAY1, DAY2, DAY3 = "2026-11-01", "2026-11-02", "2026-11-03"


def rec(op, seq, rid, status="BOOKED", arrival="2026-12-01", departure="2026-12-03", rooms="1", adr="200.00",
        currency="USD"):
    return {
        "Op": op, "_cdc_seq": str(seq), "_cdc_ts": f"2026-11-01 0{seq % 10}:00:00", "reservation_id": rid,
        "confirmation_number": "12345678", "property_id": "osm_way_1", "guest_email": f"{rid}@example.com",
        "loyalty_tier": "GOLD", "booking_channel": "BRAND_WEB", "rate_plan": "BAR", "room_type": "KING",
        "booking_ts": "2026-10-01 10:00:00", "arrival_date": arrival, "departure_date": departure, "rooms": rooms,
        "adults": "2", "currency": currency, "adr_local": adr, "total_amount_local": "400.00", "status": status,
        "cancel_ts": None, "source_system": "CRS-SIM", "updated_at": "2026-11-01 00:00:00",
        "_source_file": "x.parquet", "_ingest_date": None, "_loaded_at": "2026-11-01T00:00:00",
    }


def write_bronze(root, day, rows):
    d = root / "bronze" / "source=crs_reservations" / f"ingest_date={day}"
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{**r, "_ingest_date": day} for r in rows]).astype("string")
    df.to_parquet(d / "part-0000.parquet", index=False)


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {ICEBERG_PACKAGE} pyspark-shell"
    os.environ["ICEBERG_NAMESPACE_PREFIX"] = "test"
    from pipeline.common.spark import iceberg_session
    from pipeline.jobs import reservations_cdc_merge as m

    m.configure_tables("test")
    spark = iceberg_session("test-cdc-merge", str(root))
    spark.sparkContext.setLogLevel("ERROR")
    yield spark, root, m
    spark.stop()


def silver(spark, m):
    return {r["reservation_id"]: r for r in spark.table(m.TARGET).collect()}


def test_initial_load_and_dedupe(env):
    spark, root, m = env
    write_bronze(root, DAY1, [
        rec("I", 1, "R1"),
        rec("I", 2, "R2"),
        rec("I", 3, "R3"),
        rec("U", 4, "R3", status="MODIFIED"),                                # same-batch duplicate: latest wins
        rec("I", 5, "R4"), rec("D", 6, "R4"),                               # created + purged in same batch
    ])
    out = m.run(spark, str(root), DAY1, salt="s", max_reject_rate=0.5)
    s = silver(spark, m)
    assert out["status"] == "SUCCEEDED" and out["watermark"] == DAY1
    assert set(s) == {"R1", "R2", "R3"}                                      # R4 never materialises
    assert s["R3"]["status"] == "MODIFIED" and s["R3"]["_cdc_seq"] == 4
    assert out["rows_inserted"] == 3
    assert "@" not in (s["R1"]["guest_key"] or "") and s["R1"]["guest_key"]  # email hashed, not stored
    assert s["R1"]["nights"] == 2


def test_updates_deletes_stale_and_rejects(env):
    spark, root, m = env
    write_bronze(root, DAY2, [
        rec("U", 10, "R1", status="CANCELLED"),                             # normal update
        rec("D", 11, "R2"),                                                  # hard delete
        rec("U", 2, "R3", status="BOOKED"),                                  # late/out-of-order: older seq
        rec("I", 12, "R5", arrival="2026-12-05", departure="2026-12-04"),   # DQ: departure before arrival
        rec("I", 13, "R6", currency="JPY"),                                  # DQ: unknown currency
    ])
    write_bronze(root, DAY3, [rec("I", 20, "R7")])
    out = m.run(spark, str(root), DAY3, salt="s", max_reject_rate=0.5)       # catches up DAY2 + DAY3
    s = silver(spark, m)
    assert out["partitions_processed"] == [DAY2, DAY3] and out["watermark"] == DAY3
    assert s["R1"]["status"] == "CANCELLED"
    assert "R2" not in s
    assert s["R3"]["status"] == "MODIFIED" and s["R3"]["_cdc_seq"] == 4     # stale event ignored
    assert "R5" not in s and "R6" not in s and "R7" in s
    assert (out["rows_updated"], out["rows_deleted"], out["rows_stale_skipped"], out["rows_rejected"]) == (1, 1, 1, 2)
    reasons = {r["reservation_id"]: r["reject_reasons"] for r in spark.table(m.REJECTS).collect()}
    assert reasons == {"R5": "invalid_stay_dates", "R6": "unknown_currency"}


def test_rerun_is_noop(env):
    spark, root, m = env
    before = silver(spark, m)
    out = m.run(spark, str(root), DAY3, salt="s", max_reject_rate=0.5)
    assert out["status"] == "SKIPPED"
    assert silver(spark, m) == before


def test_reject_rate_breach_fails_without_touching_silver(env):
    spark, root, m = env
    day4 = "2026-11-04"
    write_bronze(root, day4, [rec("U", 30, "R1", status="CHECKED_IN"), rec("I", 31, "R8", rooms="0")])
    before = silver(spark, m)
    with pytest.raises(ValueError, match="reject rate"):
        m.run(spark, str(root), day4, salt="s", max_reject_rate=0.01)
    assert silver(spark, m) == before
    last = spark.sql(f"SELECT status, watermark FROM {m.RUN_LOG} ORDER BY started_at DESC LIMIT 1").first()
    assert last["status"] == "FAILED" and last["watermark"] is None
    # watermark didn't move, so the next run retries day 4 (here with a relaxed threshold)
    assert m.last_watermark(spark) == DAY3


def test_room_nights_exclude_cancelled(env):
    spark, root, m = env
    nights = spark.table(m.NIGHTS).collect()
    by_res = {}
    for n in nights:
        by_res.setdefault(n["reservation_id"], []).append(str(n["stay_date"]))
    assert "R1" not in by_res                                                # cancelled
    assert sorted(by_res["R3"]) == ["2026-12-01", "2026-12-02"]              # 2 nights, departure excluded
