"""Marriott International financial facts from SEC EDGAR (XBRL companyfacts API).

SEC requires a descriptive User-Agent with a contact address. Set it via the
SEC_USER_AGENT env var (e.g. "YourName your-email@example.com"); it is never
hard-coded. Rate limit: <= 10 requests/second; we make one request.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path

import requests

MARRIOTT_CIK = "0001048286"  # Marriott International Inc /MD/
URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{MARRIOTT_CIK}.json"

# us-gaap concepts we model. Extend freely; unknown concepts are skipped.
CONCEPTS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "EarningsPerShareDiluted",
    "LongTermDebtNoncurrent",
    "CashAndCashEquivalentsAtCarryingValue",
]

FIELDS = ["concept", "unit", "value", "period_start", "period_end", "fiscal_year",
          "fiscal_period", "form", "filed", "frame", "accession"]


def download(out_dir: Path) -> list[Path]:
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError("Set SEC_USER_AGENT (name + contact email) before calling SEC EDGAR")
    resp = requests.get(URL, headers={"User-Agent": ua, "Accept-Encoding": "gzip"}, timeout=60)
    resp.raise_for_status()
    facts = resp.json()["facts"].get("us-gaap", {})

    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "marriott_companyfacts.csv"
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for concept in CONCEPTS:
            for unit, items in facts.get(concept, {}).get("units", {}).items():
                for it in items:
                    w.writerow({
                        "concept": concept, "unit": unit, "value": it["val"],
                        "period_start": it.get("start"), "period_end": it["end"],
                        "fiscal_year": it.get("fy"), "fiscal_period": it.get("fp"),
                        "form": it.get("form"), "filed": it.get("filed"),
                        "frame": it.get("frame"), "accession": it.get("accn"),
                    })
    return [out]
