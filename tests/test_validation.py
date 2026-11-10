import json

import pandas as pd

from pipeline.common.schemas import SOURCES
from pipeline.ingestion import validate_and_load as v


def _good_sec_frame(n=20):
    return pd.DataFrame({c: ["x"] * n for c in SOURCES["sec_financials"]["required"]})


def test_valid_frame_has_no_errors():
    assert v.validate_frame(_good_sec_frame(), SOURCES["sec_financials"]) == []


def test_missing_column_and_low_rows():
    df = _good_sec_frame(3).drop(columns=["value"])
    errors = v.validate_frame(df, SOURCES["sec_financials"])
    assert any("missing required columns" in e and "value" in e for e in errors)
    assert any("below minimum" in e for e in errors)


def _land(tmp_path, monkeypatch, df):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    landing = tmp_path / "landing/source=sec_financials/ingest_date=2026-11-09"
    landing.mkdir(parents=True)
    df.to_csv(landing / "facts.csv", index=False)


def test_run_loads_bronze_with_lineage_and_manifest(tmp_path, monkeypatch):
    _land(tmp_path, monkeypatch, _good_sec_frame())
    assert v.run("sec_financials", "2026-11-09") is True

    bronze = tmp_path / "bronze/source=sec_financials/ingest_date=2026-11-09"
    out = pd.read_parquet(bronze / "part-0000.parquet")
    assert len(out) == 20
    assert {"_source_file", "_ingest_date", "_loaded_at"} <= set(out.columns)
    manifest = json.loads((bronze / "_manifest.json").read_text())
    assert manifest["files"][0]["rows_read"] == manifest["files"][0]["rows_written"] == 20


def test_run_quarantines_bad_file(tmp_path, monkeypatch):
    _land(tmp_path, monkeypatch, _good_sec_frame(2).drop(columns=["concept"]))
    assert v.run("sec_financials", "2026-11-09") is False

    q = tmp_path / "quarantine/source=sec_financials/ingest_date=2026-11-09"
    assert (q / "facts.csv").exists()
    reason = json.loads((q / "reason_0.json").read_text())
    assert any("concept" in e for e in reason["errors"])
    assert not list((tmp_path / "bronze").rglob("*.parquet"))


def test_rerun_is_idempotent(tmp_path, monkeypatch):
    _land(tmp_path, monkeypatch, _good_sec_frame())
    v.run("sec_financials", "2026-11-09")
    v.run("sec_financials", "2026-11-09")
    files = list((tmp_path / "bronze").rglob("*.parquet"))
    assert len(files) == 1
