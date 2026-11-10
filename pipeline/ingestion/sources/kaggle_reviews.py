"""Download hotel review datasets from Kaggle.

Requires a Kaggle API token at ~/.kaggle/kaggle.json (or KAGGLE_USERNAME / KAGGLE_KEY
env vars). In Jenkins/AWS, store these in a credentials store / Secrets Manager,
never in the repo.
"""
from __future__ import annotations

from pathlib import Path

DATASETS = {
    "datafiniti_reviews": {
        "slug": "datafiniti/hotel-reviews",
        # The dataset ships several CSVs; only these share the documented schema.
        "glob": "Datafiniti_Hotel_Reviews*.csv",
    },
    "europe_reviews": {
        "slug": "jiashenliu/515k-hotel-reviews-data-in-europe",
        "glob": "Hotel_Reviews.csv",
    },
}


def download(source: str, out_dir: Path) -> list[Path]:
    from kaggle.api.kaggle_api_extended import KaggleApi

    cfg = DATASETS[source]
    api = KaggleApi()
    api.authenticate()
    out_dir.mkdir(parents=True, exist_ok=True)
    api.dataset_download_files(cfg["slug"], path=str(out_dir), unzip=True, quiet=False)
    files = sorted(out_dir.glob(cfg["glob"]))
    if not files:
        raise FileNotFoundError(f"No files matching {cfg['glob']} in Kaggle dataset {cfg['slug']}")
    return files
