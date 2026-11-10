"""Write synthetic reference files (exact upstream schemas) into the landing zone.

  python scripts/make_sample_data.py --date 2026-11-09
Equivalent to running ingest.py for every reference source with USE_SAMPLE_DATA=1.
"""
import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["USE_SAMPLE_DATA"] = "1"

from pipeline.ingestion.ingest import land  # noqa: E402
from pipeline.simulator.reference_sample import WRITERS  # noqa: E402

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=date.today().isoformat())
    a = p.parse_args()
    for source in WRITERS:
        print(f"sample {source}:", land(source, a.date))
