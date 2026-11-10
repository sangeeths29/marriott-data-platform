"""PII masking for free-text review fields.

Reviews occasionally contain emails, phone numbers or room numbers. We mask them in
silver so nothing downstream (warehouse, dashboards) holds raw PII. Reviewer
usernames are replaced with a salted hash in the silver job, not here.
"""
from __future__ import annotations

import hashlib
import re

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# International-ish phone numbers: 9+ digits allowing spaces, dots, dashes, parens, leading +
PHONE_RE = re.compile(r"(?<!\w)\+?\(?\d[\d\s().-]{7,}\d(?!\w)")
ROOM_RE = re.compile(r"\broom\s*(number|no\.?|#)?\s*\d{2,5}\b", re.IGNORECASE)


def mask_text(text: str | None) -> str | None:
    if text is None:
        return None
    text = EMAIL_RE.sub("[EMAIL]", text)
    text = PHONE_RE.sub(lambda m: "[PHONE]" if sum(c.isdigit() for c in m.group()) >= 9 else m.group(), text)
    text = ROOM_RE.sub("room [ROOM]", text)
    return text


def hash_identifier(value: str | None, salt: str) -> str | None:
    if value is None or value == "":
        return None
    return hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()[:16]
