"""Step 1: download a source and land the raw files untouched in the landing zone.

  python -m pipeline.ingestion.ingest --source datafiniti_reviews --date 2026-11-09

Re-running for the same date overwrites that landing partition (idempotent).
USE_SAMPLE_DATA=1 writes synthetic files with the same schemas instead of downloading.
"""
from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import date
from pathlib import Path

from pipeline.common import storage
from pipeline.common.schemas import SOURCES
from pipeline.ingestion.sources import kaggle_reviews, osm_properties, sec_financials

log = logging.getLogger("ingest")


def fetch(source: str, tmp: Path) -> list[Path]:
    if os.environ.get("USE_SAMPLE_DATA") == "1":  # synthetic files, same schemas (demo / CI)
        from pipeline.simulator import reference_sample

        return reference_sample.download(source, tmp)
    if source in kaggle_reviews.DATASETS:
        return kaggle_reviews.download(source, tmp)
    if source == "osm_properties":
        return osm_properties.download(tmp)
    if source == "sec_financials":
        return sec_financials.download(tmp)
    raise ValueError(f"Unknown source {source}")


def land(source: str, ingest_date: str) -> list[str]:
    dest_prefix = storage.partition_path("landing", source, ingest_date)
    storage.delete_prefix(dest_prefix)
    landed = []
    with tempfile.TemporaryDirectory() as tmp:
        for f in fetch(source, Path(tmp)):
            dest = storage.join(dest_prefix, f.name)
            storage.put_file(f, dest)
            landed.append(dest)
            log.info("landed %s (%.1f MB)", dest, f.stat().st_size / 1e6)
    return landed


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True, choices=sorted(SOURCES))
    p.add_argument("--date", default=date.today().isoformat())
    a = p.parse_args()
    land(a.source, a.date)


if __name__ == "__main__":
    main()
