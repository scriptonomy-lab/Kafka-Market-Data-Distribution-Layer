"""Unit tests for the mark-to-market maths in pipeline/risk.py.

Currencies come from config/topology.toml (quote_currency, base_currency),
never from slicing the symbol string; these tests use the real topology.
"""

import pytest

from pipeline.common import Settings
from pipeline.risk import Position, mark_book, mark_position, symbols_needed, usd_per_unit

TOPOLOGY = Settings.from_env().topology


def q(bid, ask):
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}


def test_long_xxxusd_marks_at_bid_and_pnl_is_in_usd():
    pos = Position("EURUSD", "long", 5_000_000, entry_price=1.1600)
    m = mark_position(pos, {"bid": 1.1610, "ask": 1.1611, "mid": 1.16105}, usd_rate=1.0)
    # 5,000,000 EUR * (1.1610 - 1.1600) USD/EUR = 5,000 USD
    assert m["pnl_usd"] == pytest.approx(5_000.0)
    assert m["mark"] == 1.1610
    assert m["notional_usd"] == pytest.approx(5_000_000 * 1.16105)


def test_short_marks_at_ask():
    pos = Position("GBPUSD", "short", 3_000_000, entry_price=1.3400)
    m = mark_position(pos, {"bid": 1.3389, "ask": 1.3390, "mid": 1.33895}, usd_rate=1.0)
    # short gains when price falls: 3,000,000 * (1.3400 - 1.3390) = 3,000 USD
    assert m["pnl_usd"] == pytest.approx(3_000.0)
    assert m["mark"] == 1.3390


def test_usdjpy_pnl_converted_from_jpy_to_usd_via_quote_currency():
    prices = {"USDJPY": {"bid": 150.50, "ask": 150.52, "mid": 150.51}}
    book = mark_book([Position("USDJPY", "long", 2_000_000, 150.00)], prices, TOPOLOGY)
    (_pos, m) = book["rows"][0]
    # 2,000,000 USD * 0.50 JPY/USD = 1,000,000 JPY -> / 150.51 = 6,644.40 USD
    assert m["pnl_usd"] == pytest.approx(1_000_000 / 150.51)
    assert m["notional_usd"] == pytest.approx(2_000_000)


def test_crypto_quantity_is_coins_and_pnl_is_usd():
    prices = {"BTCUSD": q(101_000.0, 101_010.0)}
    book = mark_book([Position("BTCUSD", "long", 2, 100_000.0)], prices, TOPOLOGY)
    (_pos, m) = book["rows"][0]
    assert m["pnl_usd"] == pytest.approx(2 * 1_000.0)  # marked at the bid
    assert m["notional_usd"] == pytest.approx(2 * 101_005.0)


def test_stock_quantity_is_shares_and_pnl_is_usd():
    prices = {"AAPL": q(231.00, 231.02)}
    book = mark_book([Position("AAPL", "long", 5_000, 230.00)], prices, TOPOLOGY)
    (_pos, m) = book["rows"][0]
    assert m["pnl_usd"] == pytest.approx(5_000 * 1.00)


def test_usd_conversion_uses_direct_usd_pairs_only():
    prices = {"USDJPY": q(150.0, 150.0), "EURUSD": q(1.2, 1.2)}
    assert usd_per_unit("USD", {}, TOPOLOGY) == 1.0
    assert usd_per_unit("JPY", prices, TOPOLOGY) == pytest.approx(1 / 150.0)
    assert usd_per_unit("EUR", prices, TOPOLOGY) == pytest.approx(1.2)
    assert usd_per_unit("JPY", {}, TOPOLOGY) is None  # no USDJPY price yet
    assert usd_per_unit("CHF", prices, TOPOLOGY) is None  # no pair at all


def test_unseeded_unpriced_or_unconvertible_positions_are_skipped():
    quote = {"bid": 1, "ask": 1, "mid": 1}
    assert mark_position(Position("EURUSD", "long", 1, None), quote, 1.0) is None
    assert mark_position(Position("EURUSD", "long", 1, 1.1), None, 1.0) is None
    assert mark_position(Position("USDJPY", "long", 1, 150.0), quote, None) is None


def test_risk_subscribes_to_conversion_pairs_too():
    # A JPY-quoted position needs USDJPY for its USD value
    book = [Position("USDJPY", "long", 1, 150.0), Position("AAPL", "long", 1, 1.0)]
    assert symbols_needed(book, TOPOLOGY) == {"USDJPY", "AAPL"}
    topics = TOPOLOGY.topics_for(symbols_needed(book, TOPOLOGY))
    assert topics == ["marketdata.fx.quotes.v1", "marketdata.equities.us.quotes.v1"]


def test_book_totals():
    book = mark_book(
        [Position("EURUSD", "long", 1_000_000, 1.0), Position("GBPUSD", "short", 1_000_000, 1.0)],
        {"EURUSD": q(1.001, 1.002), "GBPUSD": q(0.999, 1.0)},
        TOPOLOGY,
    )
    assert book["total_pnl_usd"] == pytest.approx(1_000.0 + 0.0)
