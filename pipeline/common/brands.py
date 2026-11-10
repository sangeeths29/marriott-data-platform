"""Marriott International brand catalogue and name -> brand normalisation.

Public review/property datasets only carry a free-text hotel name, so we infer the
brand with ordered regex rules. Order matters: more specific brands come first
("JW Marriott" before "Marriott", "Element by Westin" before "Westin").

Known limitation (document it, don't hide it): generic words such as "Courtyard"
or "Renaissance" can match independent hotels. Silver keeps the raw hotel_name next
to the inferred brand so false positives can be audited (see dbt test
`assert_brand_false_positive_rate`) and rules tightened over time.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Brand:
    name: str
    tier: str  # Luxury | Premium | Select | Midscale
    pattern: re.Pattern


def _b(name: str, tier: str, pattern: str) -> Brand:
    return Brand(name, tier, re.compile(pattern, re.IGNORECASE))


# Ordered: first match wins.
BRANDS: list[Brand] = [
    _b("JW Marriott", "Luxury", r"\bjw marriott\b"),
    _b("The Ritz-Carlton", "Luxury", r"\britz[\s-]?carlton\b"),
    _b("St. Regis", "Luxury", r"\bst\.?\s?regis\b"),
    _b("EDITION", "Luxury", r"\bedition hotel\b|\bthe \w+ edition\b"),
    _b("The Luxury Collection", "Luxury", r"\bluxury collection\b"),
    _b("W Hotels", "Luxury", r"^w\s|\bw hotel\b"),
    _b("Element", "Select", r"\belement\b.*\bwestin\b|\belement hotel\b|^element\b"),
    _b("Westin", "Premium", r"\bwestin\b"),
    _b("Four Points", "Select", r"\bfour points\b"),
    _b("Sheraton", "Premium", r"\bsheraton\b"),
    _b("Le Meridien", "Premium", r"\ble\s?m[eé]ridien\b"),
    _b("Renaissance", "Premium", r"\brenaissance\b"),
    _b("Autograph Collection", "Premium", r"\bautograph collection\b"),
    _b("Tribute Portfolio", "Premium", r"\btribute portfolio\b"),
    _b("Gaylord", "Premium", r"\bgaylord\b"),
    _b("Delta Hotels", "Premium", r"\bdelta hotels?\b"),
    _b("Courtyard", "Select", r"\bcourtyard\b"),
    _b("Residence Inn", "Select", r"\bresidence inn\b"),
    _b("SpringHill Suites", "Select", r"\bspringhill suites\b"),
    _b("Fairfield", "Select", r"\bfairfield (inn|by marriott)\b"),
    _b("TownePlace Suites", "Select", r"\btowneplace suites\b"),
    _b("AC Hotels", "Select", r"\bac hotels?\b"),
    _b("Aloft", "Select", r"\baloft\b"),
    _b("Moxy", "Select", r"\bmoxy\b"),
    _b("Protea", "Select", r"\bprotea hotel\b"),
    _b("Marriott Hotels", "Premium", r"\bmarriott\b"),  # catch-all, must stay last
]


def match_brand(hotel_name: str | None) -> tuple[str, str] | None:
    """Return (brand, tier) for a hotel name, or None if it is not a Marriott brand."""
    if not hotel_name:
        return None
    name = hotel_name.strip()
    for brand in BRANDS:
        if brand.pattern.search(name):
            return brand.name, brand.tier
    return None


def brand_rows() -> list[dict]:
    """Rows for the dbt `brands` seed (keeps Python and SQL in sync)."""
    return [{"brand_name": b.name, "tier": b.tier} for b in BRANDS]
