"""Data contracts for each landed source.

`required` columns must exist or the file is quarantined. `min_rows` guards
against truncated downloads. Column names are exactly as the upstream files ship.
"""
from __future__ import annotations

SOURCES: dict[str, dict] = {
    # Kaggle: datafiniti/hotel-reviews  (Datafiniti_Hotel_Reviews*.csv, mostly US hotels)
    "datafiniti_reviews": {
        "format": "csv",
        "required": [
            "id", "name", "address", "city", "province", "country", "postalCode",
            "latitude", "longitude", "reviews.date", "reviews.rating",
            "reviews.text", "reviews.title", "reviews.username",
        ],
        "min_rows": 100,
    },
    # Kaggle: jiashenliu/515k-hotel-reviews-data-in-europe  (Hotel_Reviews.csv)
    "europe_reviews": {
        "format": "csv",
        "required": [
            "Hotel_Address", "Hotel_Name", "Review_Date", "Reviewer_Nationality",
            "Negative_Review", "Positive_Review", "Reviewer_Score", "lat", "lng",
        ],
        "min_rows": 100,
    },
    # OpenStreetMap Overpass export (written by sources/osm_properties.py)
    "osm_properties": {
        "format": "jsonl",
        "required": ["osm_id", "osm_type", "name", "lat", "lon"],
        "min_rows": 10,
    },
    # SEC EDGAR XBRL companyfacts, flattened (written by sources/sec_financials.py)
    "sec_financials": {
        "format": "csv",
        "required": ["concept", "unit", "value", "period_end", "fiscal_year", "fiscal_period", "form", "filed"],
        "min_rows": 10,
    },
    # SIMULATED reservation system, AWS DMS-style full load + CDC (pipeline/simulator/crs.py)
    "crs_reservations": {
        "format": "parquet",
        "required": [
            "Op", "_cdc_seq", "_cdc_ts", "reservation_id", "property_id", "guest_email", "loyalty_tier",
            "booking_channel", "rate_plan", "room_type", "booking_ts", "arrival_date", "departure_date",
            "rooms", "currency", "adr_local", "total_amount_local", "status", "updated_at",
        ],
        "min_rows": 0,  # a quiet day can legitimately have no changes
    },
}
