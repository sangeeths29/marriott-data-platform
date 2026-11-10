"""Step 2: validate landed files against their data contract and load to bronze.

  python -m pipeline.ingestion.validate_and_load --source datafiniti_reviews --date 2026-11-09

For every landed file:
  * contract check: required columns present, minimum row count, file parses
  * pass  -> written to bronze as Parquet (all columns as strings, plus lineage columns)
  * fail  -> copied to quarantine/ with a reason.json; the run exits non-zero so the
             scheduler alerts (see ops/runbook.md)
A manifest.json with row counts is written next to the bronze partition for
reconciliation (rows_read == rows_written).
"""
from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.common import storage
from pipeline.common.metrics import put_metric
from pipeline.common.schemas import SOURCES

log = logging.getLogger("validate_and_load")


def read_file(path: str, fmt: str) -> pd.DataFrame:
    if fmt == "csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    if fmt == "jsonl":
        return pd.read_json(path, lines=True, dtype=False).astype("string")
    if fmt == "parquet":
        return pd.read_parquet(path).astype("string")
    raise ValueError(f"Unsupported format {fmt}")


def validate_frame(df: pd.DataFrame, contract: dict) -> list[str]:
    """Return a list of human-readable contract violations (empty list == valid)."""
    errors = []
    missing = [c for c in contract["required"] if c not in df.columns]
    if missing:
        errors.append(f"missing required columns: {missing}")
    if len(df) < contract["min_rows"]:
        errors.append(f"row count {len(df)} below minimum {contract['min_rows']}")
    return errors


def add_lineage(df: pd.DataFrame, source_file: str, ingest_date: str) -> pd.DataFrame:
    df = df.copy()
    df["_source_file"] = source_file.rsplit("/", 1)[-1]
    df["_ingest_date"] = ingest_date
    df["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return df


def _local_copy(path: str, tmp: Path) -> str:
    """pandas can read local paths directly; S3 objects are downloaded first."""
    return storage.download(path, tmp)


def run(source: str, ingest_date: str) -> bool:
    contract = SOURCES[source]
    landing = storage.partition_path("landing", source, ingest_date)
    bronze = storage.partition_path("bronze", source, ingest_date)
    quarantine = storage.partition_path("quarantine", source, ingest_date)

    files = storage.list_files(landing)
    if not files:
        log.error("no landed files under %s", landing)
        return False

    storage.delete_prefix(bronze)  # idempotent re-runs
    manifest = {"source": source, "ingest_date": ingest_date, "files": []}
    ok = True
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for i, f in enumerate(files):
            entry = {"file": f}
            try:
                df = read_file(_local_copy(f, tmp), contract["format"])
                errors = validate_frame(df, contract)
            except Exception as e:  # unparseable file is a contract failure, not a crash
                df, errors = None, [f"parse error: {e!r}"]

            if errors:
                ok = False
                entry.update(status="quarantined", errors=errors)
                storage.put_file(_local_copy(f, tmp), storage.join(quarantine, f.rsplit("/", 1)[-1]))
                storage.write_json({"file": f, "errors": errors}, storage.join(quarantine, f"reason_{i}.json"))
                log.error("QUARANTINED %s: %s", f, errors)
            else:
                out = add_lineage(df, f, ingest_date)
                local_parquet = tmp / f"part-{i:04d}.parquet"
                out.to_parquet(local_parquet, index=False)
                storage.put_file(local_parquet, storage.join(bronze, local_parquet.name))
                entry.update(status="loaded", rows_read=len(df), rows_written=len(out))
                log.info("loaded %s rows from %s", len(out), f)
            manifest["files"].append(entry)

    storage.write_json(manifest, storage.join(bronze, "_manifest.json"))
    loaded = sum(e.get("rows_written", 0) for e in manifest["files"])
    put_metric("BronzeRowsLoaded", loaded)
    put_metric("QuarantinedFiles", sum(e["status"] == "quarantined" for e in manifest["files"]))
    return ok


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, choices=sorted(SOURCES))
    p.add_argument("--date", default=date.today().isoformat())
    a = p.parse_args()
    sys.exit(0 if run(a.source, a.date) else 1)


if __name__ == "__main__":
    main()
