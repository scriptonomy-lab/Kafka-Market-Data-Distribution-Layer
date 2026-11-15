"""
Unit tests for pipeline/normalize.py, driven by the example frames in the
TraderMade V2 documentation (tests/fixtures/doc_examples.jsonl).

Each test name says the rule it protects. If TraderMade changes a format,
add the new frame to the fixtures and these tests will tell you what broke.
"""

import json
from pathlib import Path

import pytest

from pipeline.common import Settings
from pipeline.normalize import (
    mask_secret,
    masked_control,
    message_kind,
    normalize_quote,
    parse_provider_ts,
    split_frame,
)

FIXTURES = Path(__file__).parent / "fixtures" / "doc_examples.jsonl"
DOC = {
    rec["_name"]: rec["frame"]
    for rec in (json.loads(line) for line in FIXTURES.read_text().splitlines() if line.strip())
}
# Provider code -> instrument, exactly what the producer passes
INSTRUMENTS = Settings.from_env().topology.by_ws_symbol


def test_every_documented_frame_is_classified():
    assert message_kind(DOC["login_ok"]) == "control"
    assert message_kind(DOC["sub_ack"]) == "control"
    assert message_kind(DOC["logout_duplicate"]) == "control"
    assert message_kind(DOC["quote_eurusd"]) == "quote"
    assert message_kind(DOC["last_quote_eurusd"]) == "snapshot"
    assert message_kind({"hello": "world"}) == "unknown"


def test_string_prices_become_floats_and_mid_is_computed():
    event, reason = normalize_quote(DOC["quote_eurusd"], INSTRUMENTS)
    assert reason is None
    assert event["bid"] == pytest.approx(1.1627)
    assert event["ask"] == pytest.approx(1.16272)
    # "m" is absent on plain QUOTE ticks, so mid must be computed
    assert event["mid"] == pytest.approx((1.1627 + 1.16272) / 2)
    assert event["bid_size"] == 100000.0
    assert event["type"] == "quote"


def test_timestamp_is_converted_to_iso_utc_with_milliseconds():
    event, _ = normalize_quote(DOC["quote_eurusd"], INSTRUMENTS)
    assert event["timestamp"] == "2026-05-15T12:36:35.588Z"
    assert parse_provider_ts("20260515-12:36:35.588").utcoffset().total_seconds() == 0


def test_last_quote_is_a_snapshot_without_sizes():
    event, reason = normalize_quote(DOC["last_quote_eurusd"], INSTRUMENTS)
    assert reason is None
    assert event["type"] == "snapshot"
    assert event["bid_size"] is None and event["ask_size"] is None


def test_ladder_payload_is_ignored_but_quote_still_parsed():
    event, reason = normalize_quote(DOC["quote_with_ladder"], INSTRUMENTS)
    assert reason is None
    assert "ladder" not in event


@pytest.mark.parametrize(
    ("patch", "reason"),
    [
        ({"s": None}, "missing_symbol"),
        ({"s": "XAUUSD"}, "unsubscribed_symbol"),
        ({"b": "abc"}, "bad_price"),
        ({"b": "0"}, "non_positive_price"),
        ({"a": "1.10", "b": "1.20"}, "crossed_quote"),
        ({"ts": "2026-05-15 12:36"}, "bad_timestamp"),
    ],
)
def test_invalid_quotes_are_rejected_with_a_reason(patch, reason):
    msg = {**DOC["quote_eurusd"], **patch}
    event, got = normalize_quote(msg, INSTRUMENTS)
    assert event is None
    assert got == reason


def test_locked_quote_is_allowed():
    msg = {**DOC["quote_eurusd"], "a": "1.16270", "b": "1.16270"}
    event, reason = normalize_quote(msg, INSTRUMENTS)
    assert reason is None and event["bid"] == event["ask"]


def test_control_messages_are_not_quotes():
    assert normalize_quote(DOC["sub_ack"], INSTRUMENTS) == (None, "not_a_quote")


def test_split_frame_handles_multiple_lines_and_garbage():
    raw = json.dumps(DOC["quote_eurusd"]) + "\n" + json.dumps(DOC["sub_ack"]) + "\nnot json\n"
    msgs = split_frame(raw)
    assert len(msgs) == 3
    assert msgs[2] == {"_unparseable": "not json"}
    assert split_frame(b'{"t":"QUOTE"}') == [{"t": "QUOTE"}]


def test_api_key_is_masked_before_logging():
    safe = masked_control(DOC["login_ok"])
    assert safe["key"] == "********wxyz"
    assert DOC["login_ok"]["key"] == "abcd1234wxyz"  # original untouched
    assert mask_secret("") == ""


def test_event_carries_internal_symbol_asset_class_and_provider_code():
    # The live feed sends "AAPL.US" (verified 2026-11-13); our symbol is "AAPL"
    msg = {**DOC["quote_eurusd"], "s": "AAPL.US", "a": "230.12", "b": "230.10"}
    event, reason = normalize_quote(msg, INSTRUMENTS)
    assert reason is None
    assert event["symbol"] == "AAPL"
    assert event["asset_class"] == "us_stocks"
    assert event["provider_symbol"] == "AAPL.US"
    assert event["schema_version"] == 2


def test_internal_symbol_is_not_accepted_as_a_provider_code():
    # "AAPL" is our name, not TraderMade's: a tick saying "AAPL" is unknown
    msg = {**DOC["quote_eurusd"], "s": "AAPL"}
    assert normalize_quote(msg, INSTRUMENTS) == (None, "unsubscribed_symbol")
