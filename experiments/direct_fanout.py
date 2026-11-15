"""
Stretch experiment E — the counterfactual: what happens WITHOUT Kafka?

The obvious alternative to a distribution layer is "direct fan-out": one
process reads the WebSocket and calls every workload in turn for each tick:

    for tick in websocket:
        pricing.update(tick)      # fast
        risk.update(tick)         # fast
        storage.write(tick)       # sometimes slow (disk, network, lock...)

This script builds exactly that, with the SAME live TraderMade feed, and
measures one thing: how stale is the price Pricing shows? ("age" = time from
receiving the tick to Pricing having applied it). For the first half of the
run storage is fast; for the second half each storage write sleeps
--slow-ms. Because everything shares one loop, a slow storage write delays
Pricing too, and ticks queue up in the socket buffer.

Compare with experiment C, where a slow Storage CONSUMER left Pricing's
event age unchanged. That contrast is the "why Kafka" argument in numbers.

IMPORTANT: stop the producer first. One TraderMade key = one session; the
script refuses to run while the producer's PID file is alive.

Output: results/<timestamp>-expE-direct-fanout/{ages.csv, summary.json, summary.md}

Run:  make fanout-baseline ARGS="--seconds 240 --slow-ms 200"
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import sys
import time
from datetime import UTC, datetime
from urllib.parse import urlparse

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pipeline.common import REPO_ROOT, Settings, producer_running, write_json_atomic
from pipeline.normalize import message_kind, normalize_quote, split_frame
from pipeline.producer import OFFICIAL_WS_HOST


async def run(settings: Settings, seconds: float, slow_ms: float, rows: list) -> None:
    instruments = settings.topology.by_ws_symbol  # provider code -> instrument
    prices: dict[str, float] = {}
    t_start = time.monotonic()
    async with connect(settings.ws_url, ping_interval=settings.ws_ping_interval_s) as ws:
        await ws.send(json.dumps({"action": "login", "key": settings.ws_key, "fmt": "JSON"}))
        await ws.send(
            json.dumps({"action": "subscribe", "symbols": [f"{c}:QUOTE" for c in instruments]})
        )
        while time.monotonic() - t_start < seconds:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
            except TimeoutError:
                continue
            # recv() hands us the oldest frame waiting in the socket buffer.
            # When the loop below is slow, frames pile up there, and their
            # wait shows up as age at the moment Pricing finally sees them.
            received = time.monotonic()
            slow_phase = (received - t_start) > seconds / 2
            for msg in split_frame(raw):
                if message_kind(msg) != "quote":
                    continue
                event, _reason = normalize_quote(msg, instruments)
                if event is None:
                    continue
                # 1) pricing — the workload we measure
                prices[event["symbol"]] = event["mid"]
                pricing_done = time.monotonic()
                # 2) risk — cheap, inline
                _ = sum(prices.values())
                # 3) storage — inline, and slow in the second half
                if slow_phase:
                    time.sleep(
                        slow_ms / 1000.0
                    )  # blocking on purpose: that's the design under test
                rows.append(
                    {
                        "t_s": round(received - t_start, 3),
                        "phase": "slow_storage" if slow_phase else "fast_storage",
                        "symbol": event["symbol"],
                        # provider -> pricing age, using TraderMade's timestamp
                        "age_vs_provider_ms": round(
                            (
                                datetime.now(UTC)
                                - datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                            ).total_seconds()
                            * 1000,
                            1,
                        ),
                        "pricing_work_ms": round((pricing_done - received) * 1000, 3),
                    }
                )


def main() -> int:
    ap = argparse.ArgumentParser(description="Direct fan-out counterfactual (no Kafka)")
    ap.add_argument("--seconds", type=float, default=240)
    ap.add_argument(
        "--slow-ms", type=float, default=200, help="storage delay per tick in the second half"
    )
    args = ap.parse_args()
    settings = Settings.from_env()
    if producer_running():
        print("Stop the producer first: one TraderMade key = one session.", file=sys.stderr)
        return 2

    rows: list[dict] = []
    closed_reason = None
    try:
        asyncio.run(run(settings, args.seconds, args.slow_ms, rows))
    except ConnectionClosed as exc:
        # A blocked loop cannot answer pings or drain the socket; if the
        # connection dies, that is itself a finding about direct fan-out.
        closed_reason = f"connection closed after {rows[-1]['t_s'] if rows else 0}s: {exc}"
        print(closed_reason, file=sys.stderr)

    simulated = urlparse(settings.ws_url).hostname != OFFICIAL_WS_HOST
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = settings.results_dir / ("_dev" if simulated else "") / f"{stamp}-expE-direct-fanout"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "ages.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["t_s"])
        w.writeheader()
        w.writerows(rows)

    def stats(phase: str) -> dict:
        ages = [r["age_vs_provider_ms"] for r in rows if r["phase"] == phase]
        if len(ages) < 2:
            return {"ticks": len(ages)}
        q = statistics.quantiles(ages, n=100)
        return {
            "ticks": len(ages),
            "age_p50_ms": round(q[49], 1),
            "age_p99_ms": round(q[98], 1),
            "age_max_ms": round(max(ages), 1),
        }

    summary = {
        "simulated_feed": simulated,
        "seconds": args.seconds,
        "slow_ms_per_tick": args.slow_ms,
        "fast_storage": stats("fast_storage"),
        "slow_storage": stats("slow_storage"),
        "connection_closed": closed_reason,
        "note": "age = wall clock minus TraderMade ts; includes network and any clock offset, "
        "so compare the two phases with each other, not with absolute targets",
    }
    write_json_atomic(out / "summary.json", summary)
    (out / "summary.md").write_text(
        f"# Experiment E (direct fan-out, no Kafka) — {stamp}\n\n"
        + ("**SIMULATED FEED — NOT EVIDENCE**\n\n" if simulated else "")
        + "| Phase | Ticks | Pricing age p50 (ms) | p99 (ms) | max (ms) |\n| --- | --- | --- | --- | --- |\n"
        + "\n".join(
            f"| {ph} | {summary[ph].get('ticks')} | {summary[ph].get('age_p50_ms')} | "
            f"{summary[ph].get('age_p99_ms')} | {summary[ph].get('age_max_ms')} |"
            for ph in ("fast_storage", "slow_storage")
        )
        + f"\n\nStorage delay in slow phase: {args.slow_ms} ms per tick.\n"
    )
    print(json.dumps(summary, indent=2))
    print(f"written to {out.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
