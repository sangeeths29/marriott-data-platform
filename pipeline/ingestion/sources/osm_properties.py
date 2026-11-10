"""Pull Marriott-brand hotel locations from OpenStreetMap via the Overpass API.

Queried per country to stay within Overpass time/size limits. Output is JSON Lines,
one hotel per line. Be a good citizen: the public Overpass endpoint is shared and
rate-limited, so this runs at most daily and sleeps between countries.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
USER_AGENT = "marriott-data-platform/0.1 (portfolio project)"

BRAND_REGEX = (
    "Marriott|Courtyard|Residence Inn|SpringHill|Fairfield Inn|TownePlace|Sheraton|Westin|"
    "Ritz-Carlton|St. Regis|Le M[eé]ridien|Renaissance|Autograph Collection|Four Points|"
    "Aloft|Element|Moxy|AC Hotel|Delta Hotels|Gaylord|Luxury Collection|EDITION|"
    "Tribute Portfolio|Protea|W Hotel"
)

QUERY_TEMPLATE = """
[out:json][timeout:180];
area["ISO3166-1"="{cc}"][admin_level=2]->.a;
(
  nwr["tourism"="hotel"]["brand"~"{regex}",i](area.a);
  nwr["tourism"="hotel"]["name"~"{regex}",i](area.a);
);
out center tags;
"""

DEFAULT_COUNTRIES = ["US", "GB", "FR", "ES", "IT", "NL", "AT"]  # overlaps the review datasets


def _record(el: dict, cc: str) -> dict:
    tags = el.get("tags", {})
    lat = el.get("lat", el.get("center", {}).get("lat"))
    lon = el.get("lon", el.get("center", {}).get("lon"))
    return {
        "osm_id": el["id"],
        "osm_type": el["type"],
        "name": tags.get("name"),
        "brand": tags.get("brand"),
        "brand_wikidata": tags.get("brand:wikidata"),
        "operator": tags.get("operator"),
        "lat": lat,
        "lon": lon,
        "street": tags.get("addr:street"),
        "housenumber": tags.get("addr:housenumber"),
        "city": tags.get("addr:city"),
        "postcode": tags.get("addr:postcode"),
        "country_code": cc,
        "stars": tags.get("stars"),
        "rooms": tags.get("rooms"),
        "website": tags.get("website"),
    }


def download(out_dir: Path, countries: list[str] | None = None) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "osm_marriott_properties.jsonl"
    seen: set[tuple[str, int]] = set()
    with out.open("w") as f:
        for cc in countries or DEFAULT_COUNTRIES:
            q = QUERY_TEMPLATE.format(cc=cc, regex=BRAND_REGEX)
            for attempt in range(3):
                resp = requests.post(OVERPASS_URL, data={"data": q}, headers={"User-Agent": USER_AGENT}, timeout=240)
                if resp.status_code in (429, 504):
                    time.sleep(30 * (attempt + 1))
                    continue
                resp.raise_for_status()
                break
            else:
                raise RuntimeError(f"Overpass failed for {cc} after retries")
            for el in resp.json().get("elements", []):
                key = (el["type"], el["id"])
                if key in seen:
                    continue
                seen.add(key)
                f.write(json.dumps(_record(el, cc)) + "\n")
            time.sleep(10)
    return [out]
