"""
The three narrow TraderMade REST calls this project makes. None of them is on
the hot path: live prices come ONLY from the WebSocket. REST is used for

    market_status()   gate experiments: refuse to record while FX is closed
    market_statuses() every market REST reports (preflight)
    live_quotes()     seed the synthetic risk book; spot-check Pricing's state

Security note
-------------
TraderMade's REST API takes the key as a query parameter (`api_key=...`), so
the full request URL contains the secret. `requests` exceptions often embed
that URL in their message. Every error raised from here is therefore rebuilt
WITHOUT the URL, so a stack trace in a log or screenshot can never leak the key.

Docs: https://tradermade.com/docs/restful-api
"""

from __future__ import annotations

import requests

from pipeline.common import Settings


class RestError(RuntimeError):
    """A TraderMade REST call failed. The message never contains the key."""


def _get(settings: Settings, endpoint: str, params: dict | None = None) -> dict:
    """GET <rest_url>/<endpoint> with the REST key; returns parsed JSON."""
    if not settings.rest_key:
        raise RestError("TRADERMADE_REST_KEY is not set in .env")
    query = dict(params or {})
    query["api_key"] = settings.rest_key
    try:
        resp = requests.get(f"{settings.rest_url}/{endpoint}", params=query, timeout=10)
    except requests.RequestException as exc:
        # Deliberately drop `exc` (its text may contain the URL with the key)
        raise RestError(f"{endpoint}: network error ({type(exc).__name__})") from None
    if resp.status_code != 200:
        raise RestError(f"{endpoint}: HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError:
        raise RestError(f"{endpoint}: response was not JSON") from None
    # TraderMade reports some errors inside a 200 response (see "Error Codes")
    if isinstance(data, dict) and data.get("errors"):
        raise RestError(f"{endpoint}: API error {data.get('message', '')}".strip())
    return data


def market_statuses(settings: Settings) -> dict:
    """The whole /market_open_status answer: one entry per market TraderMade
    reports (FX at least). Preflight shows every entry it gets, and says so
    plainly when a market we stream (crypto, US stocks) has no entry, rather
    than guessing its hours."""
    data = _get(settings, "market_open_status")
    return {k: v for k, v in data.items() if isinstance(v, dict)}


def market_status(settings: Settings) -> dict:
    """Return the FX entry of /market_open_status, e.g.
    {"instrument": "FX", "status": "open", "next_close": "2026-10-30 21:00", ...}
    (times are GMT)."""
    data = market_statuses(settings)
    fx = data.get("FX")
    if not isinstance(fx, dict):
        raise RestError("market_open_status: no FX entry in response")
    return fx


def fx_is_open(settings: Settings) -> bool:
    return market_status(settings).get("status") == "open"


def live_quotes(settings: Settings, rest_codes: tuple[str, ...]) -> dict[str, dict]:
    """Latest REST quotes, keyed by the REST code that was asked for:
    {"EURUSD": {"bid": 1.1623, "ask": 1.1624, "mid": 1.16235, "timestamp": 1761742000}, ...}

    Pass REST codes (topology.toml rest_symbol), not internal symbols; the
    callers map them back. `per_quote_ts=true` asks TraderMade for a Unix
    timestamp on every quote, so reconcile can show how fresh each price is.
    A code REST does not know comes back as a quote with "error": 400
    ("currency code is invalid") inside a 200 response; it is left out.
    """
    data = _get(settings, "live", {"currency": ",".join(rest_codes), "per_quote_ts": "true"})
    out: dict[str, dict] = {}
    for q in data.get("quotes", []):
        if q.get("error"):
            continue
        # FX and crypto quotes are split into base/quote currencies; CFDs and
        # stocks use "instrument"
        symbol = q.get("instrument") or f"{q.get('base_currency', '')}{q.get('quote_currency', '')}"
        if symbol:
            out[symbol] = {
                "bid": q.get("bid"),
                "ask": q.get("ask"),
                "mid": q.get("mid"),
                "timestamp": q.get("timestamp"),
            }
    return out
