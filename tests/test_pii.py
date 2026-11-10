from pipeline.common.pii import hash_identifier, mask_text


def test_masks_email_phone_room():
    text = "Email me at john.doe@example.com or call +1 (602) 555-0199. We were in room 1423."
    out = mask_text(text)
    assert "john.doe" not in out and "[EMAIL]" in out
    assert "555-0199" not in out and "[PHONE]" in out
    assert "1423" not in out and "[ROOM]" in out


def test_keeps_ordinary_numbers():
    text = "Stayed 3 nights in 2019, paid 250 dollars, rated it 4/5."
    assert mask_text(text) == text


def test_none_passthrough():
    assert mask_text(None) is None


def test_hash_is_stable_and_salted():
    assert hash_identifier("alice", "s1") == hash_identifier("alice", "s1")
    assert hash_identifier("alice", "s1") != hash_identifier("alice", "s2")
    assert hash_identifier("", "s1") is None
