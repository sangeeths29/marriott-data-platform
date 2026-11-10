"""SIMULATED Central Reservation System (CRS) with an AWS DMS-style CDC feed.

*** All reservation data produced here is synthetic. ***
Real hotel booking data is not public, so this module stands in for an
operational reservation database. It generates bookings for the real
Marriott-brand properties found in silver/properties (OpenStreetMap), with
realistic hospitality behaviour: seasonality, lead times, length of stay,
channel / rate-plan / loyalty mix, modifications, cancellations, no-shows.

It is event-sourced: every booking's full lifecycle is generated up front into a
local SQLite "transaction log". Exports then replay that log exactly like AWS DMS:

  init     full-load snapshot (ingest_date = start - 1), Op = 'I'
  advance  one CDC file per day with every change exported that day, Op = I/U/D

Deliberate real-world messiness, so the downstream merge has to be correct:
  * several changes to the same reservation in one file (created + modified same day)
  * ~2% of changes arrive a day late (in the next day's file)
  * rare hard deletes (test bookings purged by the source system)
  * the first CDC file re-delivers late events already covered by the full load
History starts 60 days before --start; the first week of history ramps up
(guests who arrived earlier aren't simulated), so judge KPIs on later dates.

  python -m pipeline.simulator.crs init --start 2026-11-01 --days 14
  python -m pipeline.simulator.crs advance --date 2026-11-01
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import sqlite3
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

from pipeline.common import storage
from pipeline.common.schemas import SOURCES

log = logging.getLogger("crs_simulator")

SOURCE = "crs_reservations"
SOURCE_SYSTEM = "CRS-SIM"
DEFAULT_ROOMS = 150  # used when OSM has no room count; dbt var default_room_count must match

# Approximate USD per 1 unit of local currency (static, for simulation). dbt seed fx_rates mirrors this.
FX_TO_USD = {"USD": 1.0, "GBP": 1.27, "EUR": 1.08}
COUNTRY_CURRENCY = {"US": "USD", "GB": "GBP"}  # everything else in the sample is a euro country

BASE_ADR_USD = {"Luxury": 420.0, "Premium": 215.0, "Select": 140.0, "Midscale": 105.0}
MONTH_DEMAND = [0.82, 0.86, 0.95, 1.0, 1.05, 1.12, 1.18, 1.15, 1.04, 1.0, 0.9, 0.88]

CHANNELS = {"BRAND_WEB": 0.38, "MOBILE_APP": 0.17, "OTA": 0.25, "GDS": 0.12, "VOICE": 0.08}
RATE_PLANS = {  # plan: (weight, price factor, cancel probability)
    "BAR": (0.40, 1.00, 0.20), "MEMBER": (0.25, 0.93, 0.18), "CORP": (0.15, 0.88, 0.22),
    "AAA": (0.07, 0.90, 0.18), "PACKAGE": (0.08, 1.15, 0.12), "NONREF": (0.05, 0.85, 0.02),
}
ROOM_TYPES = {"KING": (0.45, 1.00), "DBL_QUEEN": (0.35, 1.00), "CLUB": (0.12, 1.25), "SUITE": (0.08, 1.80)}
LOYALTY = {"NONE": 0.35, "MEMBER": 0.30, "SILVER": 0.12, "GOLD": 0.11, "PLATINUM": 0.08,
           "TITANIUM": 0.03, "AMBASSADOR": 0.01}
LOS = {1: 0.33, 2: 0.30, 3: 0.17, 4: 0.09, 5: 0.05, 7: 0.06}
BASE_OCCUPANCY = 0.70      # target NET occupancy (after cancellations / no-shows)
EXPECTED_ATTRITION = 0.21  # ~18% cancellations + ~3% no-shows: gross demand is grossed up by this
MAX_LEAD_DAYS = 180


def _pick(rng: random.Random, weights: dict):
    keys = list(weights)
    w = [v[0] if isinstance(v, tuple) else v for v in weights.values()]
    return rng.choices(keys, weights=w, k=1)[0]


def _ts(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(d, time(hour, minute))


def _property_code(property_id: str) -> str:
    """Stable 5-char property code (simulated hotel code)."""
    return hashlib.md5(property_id.encode()).hexdigest()[:5].upper()


def _db_path() -> Path:
    root = storage.data_root()
    base = Path("./data") if storage.is_s3(root) else Path(root)
    return base / "_simulator" / "crs.sqlite"


def load_properties(max_properties: int) -> list[dict]:
    files = [f for f in storage.list_files(storage.join(storage.data_root(), "silver", "properties"))
             if f.endswith(".parquet")]
    if not files:
        raise RuntimeError("silver/properties is empty: run the reference pipeline (bronze_to_silver) first")
    with tempfile.TemporaryDirectory() as tmp:
        local = [storage.download(f, Path(tmp)) for f in files]
        df = pd.concat(pd.read_parquet(f) for f in local)
    df = df.sort_values("property_id").head(max_properties)
    return [
        {
            "property_id": r.property_id,
            "tier": r.tier or "Select",
            "rooms": int(r.rooms) if pd.notna(r.rooms) and int(r.rooms) > 0 else DEFAULT_ROOMS,
            "currency": COUNTRY_CURRENCY.get(r.country_code, "EUR"),
        }
        for r in df.itertuples()
    ]


def booking_lifecycle(rng: random.Random, prop: dict, arrival: date, res_no: int) -> list[tuple[datetime, str, dict]]:
    """All events for one reservation: [(event_ts, op, full row image after the change)]."""
    nights = _pick(rng, LOS)
    lead = min(int(rng.expovariate(1 / 28)), MAX_LEAD_DAYS)
    booked_at = _ts(arrival - timedelta(days=lead), rng.randint(0, 23), rng.randint(0, 59))
    plan = _pick(rng, RATE_PLANS)
    room = _pick(rng, ROOM_TYPES)
    weekend_mix = 1.05 if arrival.weekday() in (4, 5) else 1.0
    adr_usd = (BASE_ADR_USD.get(prop["tier"], 140.0) * MONTH_DEMAND[arrival.month - 1] * weekend_mix
               * RATE_PLANS[plan][1] * ROOM_TYPES[room][1] * rng.uniform(0.9, 1.1))
    adr_local = round(adr_usd / FX_TO_USD[prop["currency"]], 2)
    rooms = 1 if rng.random() < 0.92 else 2
    rid = f"{_property_code(prop['property_id'])}-{res_no:08d}"

    row = {
        "reservation_id": rid,
        "confirmation_number": f"{rng.randint(10**7, 10**8 - 1)}",
        "property_id": prop["property_id"],
        "guest_email": f"guest{rng.randint(1, 400_000)}@example.com",  # synthetic; hashed + dropped in silver
        "loyalty_tier": _pick(rng, LOYALTY),
        "booking_channel": _pick(rng, CHANNELS),
        "rate_plan": plan,
        "room_type": room,
        "booking_ts": booked_at.isoformat(sep=" "),
        "arrival_date": arrival.isoformat(),
        "departure_date": (arrival + timedelta(days=nights)).isoformat(),
        "rooms": rooms,
        "adults": rng.choice([1, 1, 2, 2, 2, 3]),
        "currency": prop["currency"],
        "adr_local": adr_local,
        "total_amount_local": round(adr_local * nights * rooms, 2),
        "status": "BOOKED",
        "cancel_ts": None,
        "source_system": SOURCE_SYSTEM,
    }
    events = [(booked_at, "I", dict(row))]

    if rng.random() < 0.002:  # test booking purged by the source system
        events.append((booked_at + timedelta(hours=rng.randint(1, 20)), "D", dict(row)))
        return events

    arrive_at = _ts(arrival, 15)
    if rng.random() < 0.10 and arrive_at - booked_at > timedelta(hours=2):  # modification
        mod_at = booked_at + (arrive_at - booked_at) * rng.uniform(0.05, 0.95)
        nights = max(1, nights + rng.choice([-1, 1, 1]))
        row.update(departure_date=(arrival + timedelta(days=nights)).isoformat(), status="MODIFIED",
                   total_amount_local=round(adr_local * nights * row["rooms"], 2))
        events.append((mod_at, "U", dict(row)))

    if rng.random() < RATE_PLANS[plan][2] and arrive_at - events[-1][0] > timedelta(hours=1):
        cancel_at = events[-1][0] + (arrive_at - events[-1][0]) * rng.uniform(0.1, 0.99)
        row.update(status="CANCELLED", cancel_ts=cancel_at.isoformat(sep=" "))
        events.append((cancel_at, "U", dict(row)))
        return events

    if rng.random() < 0.03:
        row.update(status="NO_SHOW")
        events.append((_ts(arrival, 23, 30), "U", dict(row)))
        return events

    row.update(status="CHECKED_IN")
    check_in_at = max(_ts(arrival, rng.randint(14, 22)), events[-1][0] + timedelta(minutes=5))  # after any change
    events.append((check_in_at, "U", dict(row)))
    row.update(status="CHECKED_OUT")
    events.append((_ts(date.fromisoformat(row["departure_date"]), rng.randint(7, 11)), "U", dict(row)))
    return events


def init(start: date, days: int, max_properties: int, seed: int = 42) -> int:
    rng = random.Random(seed)
    props = load_properties(max_properties)
    db = _db_path()
    db.parent.mkdir(parents=True, exist_ok=True)
    db.unlink(missing_ok=True)

    events: list[tuple] = []
    res_no = 0
    first_arrival, last_arrival = start - timedelta(days=60), start + timedelta(days=days + MAX_LEAD_DAYS)
    d = first_arrival
    while d <= last_arrival:
        demand = MONTH_DEMAND[d.month - 1] * (1.06 if d.weekday() in (4, 5) else 0.97)
        for p in props:
            # 2.7 = avg nights x rooms per booking
            mean_arrivals = p["rooms"] * BASE_OCCUPANCY * demand / 2.7 / (1 - EXPECTED_ATTRITION)
            for _ in range(max(0, int(rng.gauss(mean_arrivals, mean_arrivals * 0.08)))):
                res_no += 1
                for ts, op, image in booking_lifecycle(rng, p, d, res_no):
                    if ts.date() > start + timedelta(days=days):
                        continue  # beyond the simulation horizon
                    late = op != "I" and rng.random() < 0.02  # ~2% of changes delivered a day late
                    events.append((ts, op, image, ts.date() + timedelta(days=1 if late else 0)))
        d += timedelta(days=1)

    events.sort(key=lambda e: e[0])
    con = sqlite3.connect(db)
    con.execute("create table events (seq integer primary key, reservation_id text, op text, "
                "event_ts text, export_date text, image text)")
    con.executemany(
        "insert into events (reservation_id, op, event_ts, export_date, image) values (?,?,?,?,?)",
        ((img["reservation_id"], op, ts.isoformat(sep=" "), exp.isoformat(), json.dumps(img))
         for ts, op, img, exp in events),
    )
    con.execute("create index ix_export on events(export_date)")
    con.commit()

    # Full load: latest image of every reservation as of end of (start - 1), deleted ones excluded.
    rows = con.execute(
        """select e.seq, e.op, e.event_ts, e.image from events e
           join (select reservation_id, max(seq) seq from events where event_ts < ? group by reservation_id) m
             on e.seq = m.seq
           where e.op != 'D'""", (start.isoformat(),)).fetchall()
    con.close()
    _write_export([(seq, "I", ts, img) for seq, _op, ts, img in rows], start - timedelta(days=1), "LOAD00000001")
    log.info("simulated %s reservations / %s events for %s properties; full load %s rows",
             res_no, len(events), len(props), len(rows))
    return len(rows)


def advance(day: date) -> int:
    con = sqlite3.connect(_db_path())
    rows = con.execute("select seq, op, event_ts, image from events where export_date = ? order by seq",
                       (day.isoformat(),)).fetchall()
    con.close()
    _write_export(rows, day, f"{day:%Y%m%d}-cdc")
    log.info("CDC export for %s: %s changes", day, len(rows))
    return len(rows)


def _write_export(rows: list[tuple], ingest_date: date, name: str) -> None:
    records = [{"Op": op, "_cdc_seq": seq, "_cdc_ts": ts, **json.loads(img), "updated_at": ts}
               for seq, op, ts, img in rows]
    df = pd.DataFrame.from_records(records) if records else pd.DataFrame(columns=SOURCES[SOURCE]["required"])
    dest_prefix = storage.partition_path("landing", SOURCE, ingest_date.isoformat())
    storage.delete_prefix(dest_prefix)
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / f"{name}.parquet"
        df.to_parquet(f, index=False)
        storage.put_file(f, storage.join(dest_prefix, f.name))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("init")
    i.add_argument("--start", required=True, type=date.fromisoformat)
    i.add_argument("--days", type=int, default=14)
    i.add_argument("--properties", type=int, default=12)
    a = sub.add_parser("advance")
    a.add_argument("--date", required=True, type=date.fromisoformat)
    args = p.parse_args()
    if args.cmd == "init":
        init(args.start, args.days, args.properties)
    else:
        advance(args.date)


if __name__ == "__main__":
    main()
