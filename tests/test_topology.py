"""Unit tests for config/topology.toml loading and validation
(pipeline/topology.py). A bad topology must stop a service at start-up with
a message that names the problem, never misroute ticks silently."""

import copy
import tomllib

import pytest

from pipeline.topology import DEFAULT_TOPOLOGY_FILE, TopologyError, load_topology, parse_topology

RAW = tomllib.loads(DEFAULT_TOPOLOGY_FILE.read_text())


def broken(mutate):
    raw = copy.deepcopy(RAW)
    mutate(raw)
    return raw


def test_real_topology_loads_with_three_topics_and_eight_symbols():
    t = load_topology()
    assert list(t.topics) == ["fx", "crypto", "us_stocks"]
    assert t.topic_names == [
        "marketdata.fx.quotes.v1",
        "marketdata.crypto.quotes.v1",
        "marketdata.equities.us.quotes.v1",
    ]
    assert len(t.symbols) == 8 <= t.symbol_limit


def test_routing_comes_from_asset_class_not_symbol_shape():
    t = load_topology()
    assert t.topic_for("EURUSD") == "marketdata.fx.quotes.v1"
    # BTCUSD looks like an FX pair, but it is configured as crypto
    assert t.topic_for("BTCUSD") == "marketdata.crypto.quotes.v1"
    assert t.topic_for("AAPL") == "marketdata.equities.us.quotes.v1"


def test_provider_codes_map_to_internal_symbols():
    t = load_topology()
    assert t.by_ws_symbol["AAPL.US"].symbol == "AAPL"
    assert t.instrument("USDJPY").quote_currency == "JPY"
    assert t.instrument("MSFT").rest_symbol is None  # REST had no code for it


def test_too_many_symbols_for_the_plan_is_rejected():
    with pytest.raises(TopologyError, match="allows 7"):
        parse_topology(broken(lambda r: r["limits"].update(symbol_limit=7)))


def test_symbol_without_instrument_entry_is_rejected():
    def add_unknown(r):
        r["topics"]["fx"]["symbols"].append("AUDUSD")

    with pytest.raises(TopologyError, match="AUDUSD"):
        parse_topology(broken(add_unknown))


def test_symbol_under_the_wrong_topic_is_rejected():
    def move(r):
        r["topics"]["crypto"]["symbols"].remove("BTCUSD")
        r["topics"]["fx"]["symbols"].append("BTCUSD")

    with pytest.raises(TopologyError, match="asset_class"):
        parse_topology(broken(move))


def test_instrument_in_no_topic_is_rejected():
    with pytest.raises(TopologyError, match="not listed"):
        parse_topology(broken(lambda r: r["topics"]["us_stocks"]["symbols"].remove("NVDA")))


def test_duplicate_provider_code_is_rejected():
    with pytest.raises(TopologyError, match="ws_symbol"):
        parse_topology(broken(lambda r: r["instruments"]["MSFT"].update(ws_symbol="AAPL.US")))


def test_missing_field_names_the_instrument():
    with pytest.raises(TopologyError, match="'ETHUSD'.*quote_currency"):
        parse_topology(broken(lambda r: r["instruments"]["ETHUSD"].pop("quote_currency")))
