"""
Risk consumer — consumer group "risk-svc".

Workload: mark a small SYNTHETIC position book to market on every tick and
show live P&L and exposure in USD. The point is not risk management (no
limits, no VaR, no Greeks — the brief explicitly says not to build a risk
engine); it is to show a second, completely independent workload reading the
SAME stream.

Independence, on purpose
------------------------
Risk keeps its OWN latest-price map instead of asking the Pricing service.
Duplicating a few numbers of state is far cheaper than coupling two services:
if Pricing is down, slow or redeployed, Risk does not notice. Kafka makes this
cheap because both groups receive every message.

The book (config/positions.json)
--------------------------------
    {"positions": [
        {"symbol": "EURUSD", "side": "long",  "quantity": 5000000, "entry_price": 1.16},
        {"symbol": "USDJPY", "side": "long",  "quantity": 2000000, "entry_price": 152.1},
        {"symbol": "BTCUSD", "side": "long",  "quantity": 2,       "entry_price": null},
        {"symbol": "AAPL",   "side": "long",  "quantity": 5000,    "entry_price": null}]}
quantity is in units of the instrument: base currency for FX and crypto
(EUR, BTC), shares for stocks. `make seed-book` fills entry_price from
TraderMade REST /live so P&L starts near zero. A null entry_price is filled
with the first mid this process sees.

Risk subscribes ONLY to the topics its book needs (topology.topics_for), not
to all three: a consumer reads what it uses, which is one of the things a
per-asset-class topic layout makes possible.

Mark-to-market rules
--------------------
* Longs are marked at the BID and shorts at the ASK: that is the price you
  would actually get if you closed the position now.
* P&L is first computed in the instrument's QUOTE currency
  (topology.toml quote_currency, never sliced out of the symbol string):
      long:  quantity * (bid - entry)        short: quantity * (entry - ask)
* Converted to USD with `usd_per_unit(quote_currency)`:
      USD                -> 1
      a USD/XXX pair     -> 1 / mid of that pair  (e.g. JPY via USDJPY)
      a XXX/USD pair     -> mid of that pair
  A currency with no such pair in the book's prices cannot be converted and
  the position shows as unmarked (deliberately: no third-rate triangulation).

Run:  make risk      (or: python -m pipeline.risk)
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass

from confluent_kafka import Consumer
from rich import box
from rich.live import Live
from rich.table import Table
from rich.text import Text

from pipeline.common import (
    GROUP_RISK,
    REPO_ROOT,
    OrderingChecker,
    Settings,
    StopFlag,
    consumer_config,
    get_logger,
    log_event,
)
from pipeline.topology import Topology

POSITIONS_FILE = REPO_ROOT / "config" / "positions.json"
REFRESH_EVERY_S = 1.0
STATS_EVERY_S = 10.0


@dataclass
class Position:
    symbol: str  # internal symbol (topology.toml)
    side: str  # "long" or "short"
    quantity: float  # base currency units (FX, crypto) or shares (stocks)
    entry_price: float | None  # None until seeded


def load_positions(path=POSITIONS_FILE) -> list[Position]:
    data = json.loads(path.read_text())
    return [
        Position(p["symbol"], p["side"].lower(), float(p["quantity"]), p.get("entry_price"))
        for p in data["positions"]
    ]


def usd_per_unit(ccy: str, prices: dict[str, dict], topology: Topology) -> float | None:
    """How many USD one unit of `ccy` is worth right now, from the prices this
    service has seen. Only direct USD pairs are used (see module docstring)."""
    if ccy == "USD":
        return 1.0
    for inst in topology.instruments.values():
        quote = prices.get(inst.symbol)
        if quote is None:
            continue
        if inst.base_currency == "USD" and inst.quote_currency == ccy:
            return 1.0 / quote["mid"]  # e.g. JPY: 1 / USDJPY
        if inst.base_currency == ccy and inst.quote_currency == "USD":
            return quote["mid"]  # e.g. EUR: EURUSD
    return None


def mark_position(pos: Position, quote: dict | None, usd_rate: float | None) -> dict | None:
    """Mark one position against one quote ({"bid","ask","mid"}).

    `usd_rate` is usd_per_unit(the instrument's quote currency). Returns
    {"mark", "pnl_usd", "notional_usd"} or None when it cannot be marked yet
    (no quote, no entry price, or no USD conversion rate).
    Pure function: easy to unit-test (tests/test_risk.py).
    """
    if quote is None or pos.entry_price is None or usd_rate is None:
        return None
    if pos.side == "long":
        mark = quote["bid"]
        pnl_quote_ccy = pos.quantity * (mark - pos.entry_price)
    else:
        mark = quote["ask"]
        pnl_quote_ccy = pos.quantity * (pos.entry_price - mark)
    return {
        "mark": mark,
        "pnl_usd": pnl_quote_ccy * usd_rate,
        # quantity * price is the position's value in the quote currency
        "notional_usd": pos.quantity * quote["mid"] * usd_rate,
    }


def symbols_needed(positions: list[Position], topology: Topology) -> set[str]:
    """The book's symbols plus, for every non-USD quote currency, the direct
    USD pair that converts it. Decides which topics Risk subscribes to."""
    needed = {p.symbol for p in positions}
    for p in positions:
        ccy = topology.instrument(p.symbol).quote_currency
        if ccy == "USD":
            continue
        for inst in topology.instruments.values():
            if {inst.base_currency, inst.quote_currency} == {"USD", ccy}:
                needed.add(inst.symbol)
    return needed


def mark_book(positions: list[Position], prices: dict[str, dict], topology: Topology) -> dict:
    """Mark every position; return per-position rows plus book totals."""
    rows, total_pnl, gross = [], 0.0, 0.0
    for pos in positions:
        ccy = topology.instrument(pos.symbol).quote_currency
        marked = mark_position(pos, prices.get(pos.symbol), usd_per_unit(ccy, prices, topology))
        rows.append((pos, marked))
        if marked:
            total_pnl += marked["pnl_usd"]
            gross += abs(marked["notional_usd"])
    return {"rows": rows, "total_pnl_usd": total_pnl, "gross_notional_usd": gross}


def render(book: dict, events: int, ordering: OrderingChecker, topology: Topology) -> Table:
    # Rows are formatted as plain strings first so each column's width can be
    # taken from the values it actually holds. The P&L colour is kept apart
    # (a Text style, not inline markup) so "[red]" does not count as width.
    rows, colours = [], []
    for pos, m in book["rows"]:
        d = topology.instrument(pos.symbol).price_decimals
        entry = f"{pos.entry_price:.{d}f}" if pos.entry_price is not None else "unseeded"
        label = f"{pos.side} {pos.symbol}"
        if m is None:
            rows.append((label, f"{pos.quantity:,.0f}", entry, "-", "-", "-"))
            colours.append("")
            continue
        rows.append(
            (
                label,
                f"{pos.quantity:,.0f}",
                entry,
                f"{m['mark']:.{d}f}",
                f"{m['pnl_usd']:,.2f}",
                f"{m['notional_usd']:,.0f}",
            )
        )
        colours.append("green" if m["pnl_usd"] >= 0 else "red")

    # Layout goal: a half-width tmux pane (~65 columns) shows every number in
    # full. A notional cut to "5,000,…" is worse than no number, so:
    # * box.SIMPLE_HEAD + show_edge=False: no outer border, just a rule under
    #   the header; columns are separated by a single space instead of "│".
    # * padding (0, 1) + collapse_padding + pad_edge=False: one space of
    #   padding between neighbours (not 1 + 1) and none at the pane edges.
    #   Together that is 2 chars between columns instead of 3-4.
    # * Short headers ("Qty" is in base-currency units, "P&L" and "Notional"
    #   are in USD, as the caption says): a header longer than its values
    #   would set the column width.
    table = Table(
        title="Risk  ·  group risk-svc  ·  synthetic book",
        title_justify="left",
        box=box.SIMPLE_HEAD,
        show_edge=False,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
    )
    headers = ("Position", "Qty", "Entry", "Mark", "P&L", "Notional")
    for i, header in enumerate(headers):
        # no_wrap: rich never splits a value (or "short GBPUSD") over two
        # lines. min_width = the widest value (or header): the column is never
        # measured narrower than its content. Rich still crops evenly as a last
        # resort if the whole table cannot fit, hence the tight spacing above.
        widest = max([len(header)] + [len(row[i]) for row in rows])
        table.add_column(
            header, justify="left" if i == 0 else "right", no_wrap=True, min_width=widest
        )
    for row, colour in zip(rows, colours, strict=True):  # built in lockstep above
        table.add_row(*row[:4], Text(row[4], style=colour), row[5])
    table.caption = (
        f"P&L and notional in USD · total P&L {book['total_pnl_usd']:,.2f} USD"
        f" · gross {book['gross_notional_usd']:,.0f} USD"
        f" · events {events} · seq gaps {ordering.gaps}"
    )
    return table


def main() -> int:
    settings = Settings.from_env()
    log = get_logger("risk", settings, console=False)
    stop = StopFlag()
    positions = load_positions()
    topology = settings.topology
    unknown = [p.symbol for p in positions if p.symbol not in topology.instruments]
    if unknown:
        log_event(log, "fatal", logging.ERROR, reason=f"book symbols not in topology: {unknown}")
        return 2

    consumer = Consumer(
        consumer_config(settings, GROUP_RISK, auto_commit=True, offset_reset="latest")
    )
    # Only the topics the book needs: its own symbols plus the pairs that
    # convert their quote currencies to USD (e.g. USDJPY for a JPY price).
    topics = topology.topics_for(symbols_needed(positions, topology))
    consumer.subscribe(topics)

    prices: dict[str, dict] = {}  # Risk's OWN price map (see module docstring)
    ordering = OrderingChecker()
    events = 0
    last_refresh = last_stats = time.monotonic()
    log_event(
        log, "started", group=GROUP_RISK, topics=topics, positions=[p.__dict__ for p in positions]
    )

    # screen=True: repaint the whole pane on the alternate screen each refresh,
    # so no partial old frame survives above the title (see pricing.py).
    live = Live(render(mark_book(positions, prices, topology), 0, ordering, topology), screen=True)
    try:
        with live:
            while not stop.is_set():
                msg = consumer.poll(0.2)
                if msg is not None and not msg.error():
                    event = json.loads(msg.value())
                    events += 1
                    ordering.observe(event["session_id"], event["symbol"], event["seq"])
                    prices[event["symbol"]] = {k: event[k] for k in ("bid", "ask", "mid")}
                    # Unseeded entry: lock in the first mid we see, and say so
                    for pos in positions:
                        if pos.symbol == event["symbol"] and pos.entry_price is None:
                            pos.entry_price = event["mid"]
                            log_event(
                                log,
                                "entry_from_first_tick",
                                symbol=pos.symbol,
                                entry=pos.entry_price,
                            )
                elif msg is not None:
                    log_event(log, "consumer_error", error=str(msg.error()))

                now = time.monotonic()
                if now - last_refresh >= REFRESH_EVERY_S:
                    book = mark_book(positions, prices, topology)
                    live.update(render(book, events, ordering, topology))
                    last_refresh = now
                if now - last_stats >= STATS_EVERY_S:
                    book = mark_book(positions, prices, topology)
                    log_event(
                        log,
                        "mtm",
                        events=events,
                        total_pnl_usd=round(book["total_pnl_usd"], 2),
                        gross_notional_usd=round(book["gross_notional_usd"], 2),
                        per_position={
                            p.symbol: round(m["pnl_usd"], 2)
                            for p, m in book["rows"]
                            if m is not None
                        },
                        **ordering.as_dict(),
                    )
                    last_stats = now
    finally:
        # close() commits final offsets and leaves the group cleanly. It is in
        # `finally` so the clean leave also happens when the loop or rich's
        # screen restore raises. After `tmux kill-session` (SIGHUP, see
        # StopFlag) the pane's terminal is gone, and rich's last write fails
        # with OSError EIO; without `finally` that skips close().
        consumer.close()
        log_event(log, "stopped", signal=stop.signal_name, events=events, **ordering.as_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
