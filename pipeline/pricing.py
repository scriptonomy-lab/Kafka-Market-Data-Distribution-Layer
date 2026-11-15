"""
Pricing consumer — consumer group "pricing-svc".

Workload: keep the latest bid, ask, mid and spread for every symbol, and show
them in a live terminal table. This is what a pricing engine, a quoting UI or
a reference-rate service needs: the current state, not the history.

It subscribes to ALL three topics (FX, crypto, US stocks): one consumer
group can read several topics, and Kafka balances their partitions across
the group's members exactly as it would for one topic. Display precision and
the spread unit (FX pips, otherwise ticks) come from config/topology.toml.

Kafka choices for this workload
-------------------------------
* auto_commit=True     Pricing holds "latest value" state. If it restarts and
                       re-reads a few seconds of ticks, the end state is the
                       same, so automatic commits every second are fine.
* offset_reset=latest  A brand-new pricing group only needs the present; it
                       does not need to chew through 7 days of history.
                       (After its first commit, restarts resume from the
                       committed offset regardless of this setting.)

What it measures (written to logs/pricing.log every 10 s)
---------------------------------------------------------
* event age = now - Kafka message timestamp. The producer and this consumer
  run on the same host and therefore share one clock, so this is an honest
  measure of the producer -> Kafka -> consumer hop. It is NOT feed latency
  (TraderMade -> us), which would need synchronized clocks across companies.
* ordering = seq gaps / backward steps per symbol (expect 0 / 0).

It also writes run/pricing_state.json every second so scripts/reconcile.py
can compare our latest prices with TraderMade's REST /live endpoint.

Run:  make pricing      (or: python -m pipeline.pricing)
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from collections import deque

from confluent_kafka import Consumer
from rich import box
from rich.live import Live
from rich.table import Table

from pipeline.common import (
    GROUP_PRICING,
    OrderingChecker,
    Settings,
    StopFlag,
    consumer_config,
    get_logger,
    log_event,
    now_ms,
    write_json_atomic,
)
from pipeline.topology import Topology

STATS_EVERY_S = 10.0  # cadence of the "stats" log line
REFRESH_EVERY_S = 1.0  # table redraw + state file write
# Short asset-class labels for the table (keeps the pane narrow)
CLASS_LABEL = {"fx": "FX", "crypto": "Crypto", "us_stocks": "Stock"}


def percentile(values: list[float], pct: float) -> float | None:
    """Simple percentile (pct in 0..100) without numpy; None for empty input."""
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    # statistics.quantiles splits into 100 buckets -> 99 cut points
    return statistics.quantiles(values, n=100, method="inclusive")[int(pct) - 1]


def render(
    state: dict, ages_ms: deque, ordering: OrderingChecker, total: int, topology: Topology
) -> Table:
    """Build the terminal table from current state (pure display code)."""
    # Rows are formatted first so each column's width can be taken from the
    # values it actually holds (see the min_width comment below). Rows follow
    # the topology's order (FX, crypto, stocks), not the alphabet.
    rows = []
    for symbol in [s for s in topology.symbols if s in state]:
        q = state[symbol]
        inst = topology.instrument(symbol)
        d = inst.price_decimals
        rows.append(
            (
                CLASS_LABEL.get(inst.asset_class, inst.asset_class),
                symbol,
                f"{q['bid']:.{d}f}",
                f"{q['ask']:.{d}f}",
                f"{q['mid']:.{d + 1}f}",
                f"{(q['ask'] - q['bid']) / inst.pip_or_tick_size:.1f}",
                str(q["ticks"]),
                q["timestamp"][11:23],  # HH:MM:SS.mmm
            )
        )

    # Layout goal: a half-width tmux pane (~65 columns) shows every number in
    # full. A price cut to "1.11…" is worse than no price, so:
    # * box.SIMPLE_HEAD + show_edge=False: no outer border, just a rule under
    #   the header; columns are separated by a single space instead of "│".
    # * padding (0, 1) + collapse_padding + pad_edge=False: one space of
    #   padding between neighbours (not 1 + 1) and none at the pane edges.
    #   Together that is 2 chars between columns instead of 3-4.
    # * Short headers ("Sym", "Spread" in pips, "Time UTC" = provider time):
    #   a header longer than its values would set the column width.
    table = Table(
        title="Pricing  ·  group pricing-svc",
        title_justify="left",
        box=box.SIMPLE_HEAD,
        show_edge=False,
        padding=(0, 1),
        collapse_padding=True,
        pad_edge=False,
    )
    headers = ("Class", "Sym", "Bid", "Ask", "Mid", "Spread", "Ticks", "Time UTC")
    for i, header in enumerate(headers):
        # no_wrap: rich never splits a value over two lines. min_width = the
        # widest value (or header): the column is never measured narrower than
        # its content. Rich still crops evenly as a last resort if the whole
        # table cannot fit, which is why the spacing above is kept so tight.
        widest = max([len(header)] + [len(row[i]) for row in rows])
        table.add_column(
            header, justify="left" if i < 2 else "right", no_wrap=True, min_width=widest
        )
    for row in rows:
        table.add_row(*row)
    p50, p99 = percentile(list(ages_ms), 50), percentile(list(ages_ms), 99)
    age = f"p50 {p50:.0f} ms · p99 {p99:.0f} ms" if p50 is not None else "waiting for ticks"
    table.caption = (
        f"spread in pips (FX) or ticks · events {total} · Kafka-hop age "
        f"(last {len(ages_ms)}) {age} · seq gaps {ordering.gaps} · backward {ordering.backward}"
    )
    return table


def main() -> int:
    settings = Settings.from_env()
    # console=False: the rich table owns the terminal; logs go to the file only
    log = get_logger("pricing", settings, console=False)
    stop = StopFlag()

    consumer = Consumer(
        consumer_config(settings, GROUP_PRICING, auto_commit=True, offset_reset="latest")
    )
    topics = settings.topology.topic_names
    consumer.subscribe(topics)

    state: dict[str, dict] = {}  # symbol -> latest quote + tick count
    ages_ms: deque[float] = deque(maxlen=2000)  # recent Kafka-hop ages
    ordering = OrderingChecker()
    total = 0
    last_refresh = last_stats = time.monotonic()
    log_event(log, "started", group=GROUP_PRICING, topics=topics)

    # screen=True draws on the terminal's alternate screen and repaints the
    # whole pane each refresh. Without it, rich moves the cursor up by the
    # previous height and redraws; if the table ever wraps or exceeds a small
    # tmux pane, part of the old frame is left behind above the new one.
    live = Live(render(state, ages_ms, ordering, 0, settings.topology), screen=True)
    try:
        with live:
            while not stop.is_set():
                # poll() returns at most one message, or None after 0.2 s idle
                msg = consumer.poll(0.2)
                if msg is not None:
                    if msg.error():
                        log_event(log, "consumer_error", error=str(msg.error()))
                    else:
                        event = json.loads(msg.value())
                        total += 1
                        # msg.timestamp() -> (type, epoch_ms); type 1 = CreateTime
                        ages_ms.append(now_ms() - msg.timestamp()[1])
                        ordering.observe(event["session_id"], event["symbol"], event["seq"])
                        prev = state.get(event["symbol"], {"ticks": 0})
                        state[event["symbol"]] = {
                            "topic": msg.topic(),
                            "bid": event["bid"],
                            "ask": event["ask"],
                            "mid": event["mid"],
                            "timestamp": event["timestamp"],
                            "ticks": prev["ticks"] + 1,
                        }

                now = time.monotonic()
                if now - last_refresh >= REFRESH_EVERY_S:
                    live.update(render(state, ages_ms, ordering, total, settings.topology))
                    write_json_atomic(settings.run_dir / "pricing_state.json", state)
                    last_refresh = now
                if now - last_stats >= STATS_EVERY_S:
                    ages = list(ages_ms)
                    log_event(
                        log,
                        "stats",
                        events=total,
                        age_p50_ms=percentile(ages, 50),
                        age_p99_ms=percentile(ages, 99),
                        latest={s: q["mid"] for s, q in state.items()},
                        **ordering.as_dict(),
                    )
                    last_stats = now
    finally:
        # close() commits final offsets (auto-commit mode) and leaves the group
        # cleanly, so the broker rebalances immediately instead of waiting for
        # the session timeout. It is in `finally` so the clean leave also
        # happens when the loop or rich's screen restore raises. After
        # `tmux kill-session` (SIGHUP, see StopFlag) the pane's terminal is
        # gone, and rich's last write fails with OSError EIO; without
        # `finally` that skips close().
        consumer.close()
        log_event(log, "stopped", signal=stop.signal_name, events=total, **ordering.as_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
