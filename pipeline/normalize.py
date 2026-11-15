"""
Raw TraderMade WebSocket V2 frames -> clean internal events.

Everything in this module is a PURE function: no network, no Kafka, no clock
(except where a timestamp is parsed). That makes it trivially unit-testable
against recorded frames (tests/test_normalize.py) and keeps the producer's
I/O loop short.

What TraderMade sends (from the V2 docs, verified 2026-10-28)
-------------------------------------------------------------
Control messages always carry a "type" key:
    {"type":"login_ok","key":"...","fmt":"JSON","symbol_limit":54,"cfds":true,"trader_ladder":false}
    {"type":"login_reject","key":"...","reason":"unknown_key"}
    {"type":"sub_ack","accepted":[...],"denied":[...],"denied_reasons":{...},"invalid":[...]}
    {"type":"unsub_ack","removed":[...],"invalid":[...]}
    {"type":"logout","key":"...","reason":"duplicate_login"}
    {"type":"error","reason":"unknown_action","action":"ping"}

Market data carries a "t" key instead:
    {"a":"1.162720000","av":"100000","b":"1.162700000","bv":"100000",
     "s":"EURUSD","t":"QUOTE","ts":"20260515-12:36:35.588"}
    {"a":"1.1621","b":"1.16207","m":"1.162085","s":"EURUSD",
     "t":"LAST_QUOTE","ts":"20260515-12:39:21.783"}

Quirks this module absorbs so nothing downstream has to:
    * the provider's code ("AAPL.US") is not our symbol ("AAPL"): the tick's
      "s" is looked up in the topology (config/topology.toml) and the event
      carries the INTERNAL symbol plus its asset class, so routing and
      partitioning never depend on provider naming
    * prices and sizes arrive as STRINGS ("1.162720000")
    * the mid "m" is present only on LAST_QUOTE and ladder ticks
    * "ts" is "YYYYMMDD-HH:MM:SS.sss" with no timezone marker
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from pipeline.common import iso_ms
from pipeline.topology import Instrument

# Format of TraderMade's "ts" field, e.g. "20260515-12:36:35.588".
PROVIDER_TS_FORMAT = "%Y%m%d-%H:%M:%S.%f"

# Bump this when the event layout changes in a way consumers must know about.
# v2 (topology rework) ADDED asset_class and provider_symbol. Adding fields is
# backwards compatible: consumers read the fields they need and ignore the
# rest, so the topic name keeps its ".v1" suffix (that suffix changes only
# for a breaking change, such as a renamed or re-typed field).
SCHEMA_VERSION = 2


def split_frame(raw: str | bytes) -> list[dict]:
    """Parse one WebSocket frame into a list of JSON objects.

    In JSON mode a frame normally holds exactly one object. We still split on
    newlines defensively: if the server ever batches several objects into one
    frame, they are all handled instead of failing the whole frame.
    Unparseable lines come back as {"_unparseable": "<first 200 chars>"} so
    the producer can count them as rejects rather than crash.
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    out: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            out.append({"_unparseable": line[:200]})
            continue
        if isinstance(obj, dict):
            out.append(obj)
        else:
            out.append({"_unparseable": line[:200]})
    return out


def message_kind(msg: dict) -> str:
    """Classify a parsed message.

    Returns:
        "quote"    a live tick (t == "QUOTE")
        "snapshot" the cached last tick sent once per symbol after a subscribe
                   with send_last=true (t == "LAST_QUOTE")
        "control"  login_ok, sub_ack, logout, error, ... (has a "type" key)
        "unknown"  anything else (counted, never published)
    """
    t = msg.get("t")
    if t == "QUOTE":
        return "quote"
    if t == "LAST_QUOTE":
        return "snapshot"
    if "type" in msg:
        return "control"
    return "unknown"


def parse_provider_ts(value: str) -> datetime:
    """Parse TraderMade's "ts" into a timezone-aware datetime.

    ASSUMPTION: the timestamp is UTC. The docs do not say so explicitly, so
    `make capture` measures (local receive time - ts) on Day 1; a median near
    zero milliseconds confirms UTC. If it came out near a whole number of
    hours, this is the single place to fix.
    """
    return datetime.strptime(value, PROVIDER_TS_FORMAT).replace(tzinfo=UTC)


def _to_float(value) -> float | None:
    """'1.16272' -> 1.16272; None/''/garbage -> None (never raises)."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_quote(
    msg: dict, instruments: dict[str, Instrument]
) -> tuple[dict | None, str | None]:
    """Validate one QUOTE / LAST_QUOTE message and build the internal event.

    `instruments` maps the provider code (the tick's "s", e.g. "AAPL.US") to
    our instrument; build it with `topology.by_ws_symbol`.

    Returns (event, None) on success or (None, reject_reason) on failure, so
    the caller can count rejects by reason. Reasons, in the order checked:
        not_a_quote, missing_symbol, unsubscribed_symbol, bad_price,
        non_positive_price, crossed_quote, bad_timestamp

    The returned event holds the MARKET fields only. The producer adds the
    PIPELINE fields (seq, session_id, ingest_ts, source) because those
    depend on producer state, which this pure function must not touch.
    """
    kind = message_kind(msg)
    if kind not in ("quote", "snapshot"):
        return None, "not_a_quote"

    code = msg.get("s")
    if not code:
        return None, "missing_symbol"
    inst = instruments.get(code)
    if inst is None:
        return None, "unsubscribed_symbol"

    bid = _to_float(msg.get("b"))
    ask = _to_float(msg.get("a"))
    if bid is None or ask is None:
        return None, "bad_price"
    if bid <= 0 or ask <= 0:
        return None, "non_positive_price"
    if ask < bid:
        # A crossed quote (ask below bid) is not a tradable market. We reject
        # it rather than "fix" it: silently repairing data hides feed problems.
        # A *locked* quote (ask == bid) is allowed; the producer counts those.
        return None, "crossed_quote"

    try:
        provider_ts = parse_provider_ts(msg["ts"])
    except (KeyError, TypeError, ValueError):
        return None, "bad_timestamp"

    event = {
        "schema_version": SCHEMA_VERSION,
        "type": kind,  # "quote" | "snapshot"
        "symbol": inst.symbol,  # internal symbol = the Kafka key
        "asset_class": inst.asset_class,  # decides the topic
        "provider_symbol": code,  # what TraderMade called it, for audits
        "bid": bid,
        "ask": ask,
        # Always computed here, because "m" is missing on plain QUOTE ticks.
        # Rounded to 10 dp only to drop float noise like 1.1623249999999.
        "mid": round((bid + ask) / 2, 10),
        # Sizes are optional: LAST_QUOTE carries none.
        "bid_size": _to_float(msg.get("bv")),
        "ask_size": _to_float(msg.get("av")),
        # The brief's example calls the provider time "timestamp"; keep that name.
        "timestamp": iso_ms(provider_ts),
    }
    return event, None


def mask_secret(value: str, keep: int = 4) -> str:
    """'abcd1234wxyz' -> '********wxyz'. Used before logging anything that
    echoes the API key (TraderMade's login_ok does)."""
    if not value:
        return value
    return "*" * max(0, len(value) - keep) + value[-keep:]


def masked_control(msg: dict) -> dict:
    """Copy of a control message that is safe to log or screenshot: the
    "key" field (present on login_ok, login_reject and logout) is masked."""
    safe = dict(msg)
    if isinstance(safe.get("key"), str):
        safe["key"] = mask_secret(safe["key"])
    return safe
