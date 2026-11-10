"""Synthetic reference-data files with the exact upstream schemas.

Used when USE_SAMPLE_DATA=1 (ingest.py) or via scripts/make_sample_data.py, so the
whole pipeline runs without Kaggle credentials or network access. Values are fake;
column names/formats match the real Kaggle / OpenStreetMap / SEC files.
"""
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(7)

US_HOTELS = [  # (name, address, city, province, lat, lon)
    ("Courtyard by Marriott Boston Downtown", "275 Tremont St", "Boston", "MA", 42.3497, -71.0656),
    ("JW Marriott Chicago", "151 W Adams St", "Chicago", "IL", 41.8795, -87.6320),
    ("Residence Inn Seattle Downtown", "800 Fairview Ave N", "Seattle", "WA", 47.6265, -122.3289),
    ("Hilton Garden Inn Boston", "100 Boylston St", "Boston", "MA", 42.3520, -71.0640),  # non-Marriott
]
EU_HOTELS = [  # (name, address, lat, lng)
    ("Sheraton Grand London Park Lane", "Piccadilly Mayfair Westminster Borough London W1J 7BX United Kingdom",
     51.5049, -0.1458),
    ("Le Meridien Vienna", "Robert Stolz Platz 1 01 Innere Stadt 1010 Vienna Austria", 48.2029, 16.3675),
    ("Hotel Arena", "s Gravesandestraat 51 Oost 1092 AA Amsterdam Netherlands", 52.3600, 4.9155),  # non-Marriott
]
GOOD = ["Great staff and spotless room.", "Loved the location, breakfast was excellent.", "Friendly check-in."]
BAD = ["Room was dirty and the AC was broken.", "Terrible service, waited an hour.", "Noisy and overpriced."]


def rand_day() -> date:
    return date(2016, 1, 1) + timedelta(days=random.randint(0, 3 * 365))


def write_datafiniti(d: Path, n=200):
    cols = ["id", "dateAdded", "dateUpdated", "address", "categories", "primaryCategories", "city", "country",
            "keys", "latitude", "longitude", "name", "postalCode", "province", "reviews.date", "reviews.dateSeen",
            "reviews.rating", "reviews.sourceURLs", "reviews.text", "reviews.title", "reviews.userCity",
            "reviews.userProvince", "reviews.username", "sourceURLs", "websites"]
    with (d / "Datafiniti_Hotel_Reviews.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for i in range(n):
            name, addr, city, prov, lat, lon = random.choice(US_HOTELS)
            good = random.random() < 0.7
            text = random.choice(GOOD if good else BAD)
            if i % 25 == 0:
                text += " Contact me at guest@example.com"
            w.writerow({c: "" for c in cols} | {
                "id": f"AV{i:05d}", "address": addr, "city": city, "country": "US", "latitude": lat,
                "longitude": lon, "name": name, "postalCode": "00000", "province": prov,
                "reviews.date": f"{rand_day()}T00:00:00Z",
                "reviews.rating": random.randint(4, 5) if good else random.randint(1, 2),
                "reviews.text": text, "reviews.title": "Stay", "reviews.username": f"user{i % 40}",
            })


def write_europe(d: Path, n=200):
    cols = ["Hotel_Address", "Additional_Number_of_Scoring", "Review_Date", "Average_Score", "Hotel_Name",
            "Reviewer_Nationality", "Negative_Review", "Review_Total_Negative_Word_Counts", "Total_Number_of_Reviews",
            "Positive_Review", "Review_Total_Positive_Word_Counts", "Total_Number_of_Reviews_Reviewer_Has_Given",
            "Reviewer_Score", "Tags", "days_since_review", "lat", "lng"]
    with (d / "Hotel_Reviews.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for _ in range(n):
            name, addr, lat, lng = random.choice(EU_HOTELS)
            day = rand_day()
            w.writerow({c: "" for c in cols} | {
                "Hotel_Address": addr, "Review_Date": f"{day.month}/{day.day}/{day.year}", "Hotel_Name": name,
                "Reviewer_Nationality": random.choice([" United Kingdom ", " Germany ", " United States "]),
                "Negative_Review": random.choice(["No Negative", " " + random.choice(BAD)]),
                "Positive_Review": random.choice(["No Positive", " " + random.choice(GOOD)]),
                "Reviewer_Score": round(random.uniform(4, 10), 1), "lat": lat, "lng": lng,
            })


def write_osm(d: Path):
    rows = [
        ("Courtyard Boston Downtown", "Courtyard", 42.3499, -71.0654, "Boston", "US"),
        ("JW Marriott Chicago", "JW Marriott", 41.8796, -87.6321, "Chicago", "US"),
        ("Sheraton Grand London Park Lane", "Sheraton", 51.5050, -0.1460, "London", "GB"),
        ("Westin Seattle", "Westin", 47.6136, -122.3381, "Seattle", "US"),
        ("Moxy Chicago Downtown", "Moxy", 41.8933, -87.6315, "Chicago", "US"),
        ("Aloft London Excel", "Aloft", 51.5079, 0.0285, "London", "GB"),
        ("Marriott Vienna", "Marriott", 48.2050, 16.3790, "Vienna", "AT"),
        ("AC Hotel Barcelona Forum", "AC Hotels", 41.4110, 2.2210, "Barcelona", "ES"),
        ("Four Points by Sheraton Amsterdam", "Four Points", 52.3800, 4.8400, "Amsterdam", "NL"),
        ("Renaissance Paris Republique", "Renaissance", 48.8680, 2.3640, "Paris", "FR"),
        ("Element Amsterdam", "Element", 52.3400, 4.8700, "Amsterdam", "NL"),
    ]
    with (d / "osm_marriott_properties.jsonl").open("w") as f:
        for i, (name, brand, lat, lon, city, cc) in enumerate(rows):
            f.write(json.dumps({
                "osm_id": 1000 + i, "osm_type": "way", "name": name, "brand": brand, "brand_wikidata": None,
                "operator": None, "lat": lat, "lon": lon, "street": None, "housenumber": None, "city": city,
                "postcode": None, "country_code": cc, "stars": "4", "rooms": str(200 + i * 10), "website": None,
            }) + "\n")


def write_sec(d: Path):
    cols = ["concept", "unit", "value", "period_start", "period_end", "fiscal_year", "fiscal_period", "form",
            "filed", "frame", "accession"]
    q_end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    q_start = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    with (d / "marriott_companyfacts.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for y in range(2015, 2020):
            for q in range(1, 5):
                rev = 4.5e9 + (y - 2015) * 2e8 + q * 5e7
                for concept, val in [("Revenues", rev), ("OperatingIncomeLoss", rev * 0.12),
                                     ("NetIncomeLoss", rev * 0.08)]:
                    w.writerow({"concept": concept, "unit": "USD", "value": val,
                                "period_start": f"{y}-{q_start[q]}", "period_end": f"{y}-{q_end[q]}",
                                "fiscal_year": y, "fiscal_period": f"Q{q}", "form": "10-Q",
                                "filed": f"{y}-{q_end[q]}", "frame": f"CY{y}Q{q}", "accession": f"acc-{y}-{q}"})


WRITERS = {"datafiniti_reviews": write_datafiniti, "europe_reviews": write_europe,
           "osm_properties": write_osm, "sec_financials": write_sec}


def download(source: str, out_dir: Path) -> list[Path]:
    """Same contract as the real source modules: write files into out_dir, return them."""
    random.seed(7)
    out_dir.mkdir(parents=True, exist_ok=True)
    before = set(out_dir.iterdir())
    WRITERS[source](out_dir)
    return sorted(set(out_dir.iterdir()) - before)
