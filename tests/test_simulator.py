import csv
import random
from datetime import date
from pathlib import Path

import pytest

from pipeline.simulator import crs

PROP = {"property_id": "osm_way_1", "tier": "Premium", "rooms": 150, "currency": "EUR"}
VALID_NEXT = {
    "I": {"MODIFIED", "CANCELLED", "NO_SHOW", "CHECKED_IN", "D"},
    "MODIFIED": {"CANCELLED", "NO_SHOW", "CHECKED_IN"},
    "CHECKED_IN": {"CHECKED_OUT"},
}


@pytest.fixture(scope="module")
def lifecycles():
    rng = random.Random(1)
    return [crs.booking_lifecycle(rng, PROP, date(2026, 12, 1), i) for i in range(3000)]


def test_events_are_time_ordered_and_start_with_insert(lifecycles):
    for ev in lifecycles:
        assert ev[0][1] == "I" and ev[0][2]["status"] == "BOOKED"
        assert [t for t, _, _ in ev] == sorted(t for t, _, _ in ev)


def test_status_transitions_are_valid(lifecycles):
    for ev in lifecycles:
        prev = "I"
        for _, op, img in ev[1:]:
            nxt = "D" if op == "D" else img["status"]
            assert nxt in VALID_NEXT[prev], (prev, nxt)
            prev = nxt


def test_stay_and_money_invariants(lifecycles):
    for ev in lifecycles:
        for _, _, img in ev:
            assert img["departure_date"] > img["arrival_date"]
            assert img["adr_local"] > 0 and img["currency"] == "EUR"
            nights = (date.fromisoformat(img["departure_date"]) - date.fromisoformat(img["arrival_date"])).days
            assert img["total_amount_local"] == pytest.approx(img["adr_local"] * nights * img["rooms"], abs=0.02)
        cancel = [img for _, _, img in ev if img["status"] == "CANCELLED"]
        if cancel:
            assert cancel[0]["cancel_ts"] < "2026-12-01 15:00"  # cancelled before arrival


def test_realistic_mix(lifecycles):
    final = [ev[-1] for ev in lifecycles]
    cancel_rate = sum(img["status"] == "CANCELLED" for _, _, img in final) / len(final)
    assert 0.12 < cancel_rate < 0.25
    assert sum(op == "D" for op, in [(e[1],) for e in final]) < 30  # purges are rare


def test_reservation_id_is_stable_and_unique():
    rng = random.Random(1)
    ids = {crs.booking_lifecycle(rng, PROP, date(2026, 12, 1), i)[0][2]["reservation_id"] for i in range(500)}
    assert len(ids) == 500
    assert crs._property_code("osm_way_1") == crs._property_code("osm_way_1")


def test_fx_seed_in_sync_with_simulator():
    seed = Path(__file__).parents[1] / "dbt" / "seeds" / "fx_rates.csv"
    with seed.open() as f:
        rows = {r["currency"]: float(r["usd_per_unit"]) for r in csv.DictReader(f)}
    assert rows == crs.FX_TO_USD, "regenerate dbt/seeds/fx_rates.csv from crs.FX_TO_USD"
