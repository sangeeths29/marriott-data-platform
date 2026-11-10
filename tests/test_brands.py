import csv
from pathlib import Path

import pytest

from pipeline.common.brands import brand_rows, match_brand


@pytest.mark.parametrize("name,brand", [
    ("JW Marriott Essex House New York", "JW Marriott"),
    ("New York Marriott Marquis", "Marriott Hotels"),
    ("The Ritz-Carlton, Chicago", "The Ritz-Carlton"),
    ("Courtyard by Marriott Boston Downtown", "Courtyard"),
    ("Residence Inn Seattle Downtown/Lake Union", "Residence Inn"),
    ("Sheraton Grand London Park Lane", "Sheraton"),
    ("Le Méridien Vienna", "Le Meridien"),
    ("Element by Westin Denver", "Element"),
    ("The Westin Paris Vendome", "Westin"),
    ("Fairfield Inn & Suites Dallas", "Fairfield"),
    ("W Barcelona", "W Hotels"),
    ("St. Regis Rome", "St. Regis"),
    ("Moxy London Excel", "Moxy"),
])
def test_known_brands(name, brand):
    assert match_brand(name)[0] == brand


@pytest.mark.parametrize("name", ["Hilton Garden Inn", "Holiday Inn Express", "Hotel Arena Amsterdam", "", None,
                                  "Fairfield County Motel", "Wyndham Grand"])
def test_non_marriott(name):
    assert match_brand(name) is None


def test_specific_brand_wins_over_catch_all():
    # "Marriott" catch-all must not shadow sub-brands that contain the word
    assert match_brand("Courtyard by Marriott")[0] == "Courtyard"
    assert match_brand("JW Marriott Hotel")[0] == "JW Marriott"


def test_dbt_seed_in_sync_with_python():
    seed = Path(__file__).parents[1] / "dbt" / "seeds" / "brands.csv"
    with seed.open() as f:
        assert list(csv.DictReader(f)) == brand_rows(), "regenerate dbt/seeds/brands.csv from brands.py"
