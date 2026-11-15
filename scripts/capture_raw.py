"""
Day 0 / Day 1 reconnaissance: record raw TraderMade WebSocket V2 frames and
answer the questions the design depends on, with measurements instead of
guesses.

What it records
    data/raw/<UTC time>.jsonl          one line per frame:
                                       {"recv": "<local receive time, UTC>", "frame": "<raw text>"}
    data/raw/<UTC time>.summary.json   the report printed at the end

What the report answers
    0. (v2) Which WebSocket codes does the server accept for crypto and US
       stocks? `--symbols` tries any list of codes and the report shows the
       sub_ack (accepted / denied / invalid) plus the "s" values that really
       arrive on ticks. `--list-symbols` also asks {"action":"symbols"} and
       saves the reply. The verified codes go into config/topology.toml.
    1. Does login/subscribe work, and what does the plan allow? (login_ok, sub_ack)
    2. How fast do ticks arrive per symbol? (ticks/s, longest silence)
       -> sets STALE_AFTER_S and tells you what lag numbers to expect
    3. Is the "ts" field UTC? Median of (receive time - ts read as UTC):
       a few ms to a few hundred ms  -> UTC, as assumed in normalize.py
       close to a whole number of hours -> another timezone: fix normalize.py
       (Requires your computer clock to be NTP-synced, which is the default.)
    4. Are WebSocket protocol pings answered? With pings on (default), a
       server that ignores them causes "keepalive ping timeout" disconnects.
       The report lists every disconnect and its reason.

The API key is masked in every frame before it is written to disk.
Do NOT run this while `make producer` runs: one key = one session.

Run:  make capture                 (10 minutes, every symbol in topology.toml)
      make capture ARGS="--minutes 60"
      make capture ARGS="--minutes 1 --list-symbols --symbols BTCUSD,AAPL,AAPL.US"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pipeline.common import REPO_ROOT, Settings, iso_ms, producer_running, utc_now
from pipeline.normalize import masked_control, message_kind, parse_provider_ts, split_frame


def mask_frame(raw: str) -> str:
    """Re-serialize control messages with the key masked; leave ticks as-is."""
    msgs = split_frame(raw)
    if any(message_kind(m) == "control" for m in msgs):
        return "\n".join(json.dumps(masked_control(m)) for m in msgs)
    return raw


async def wait_for_type(ws, wanted: str | None, timeout: float, out, seen: list) -> dict | None:
    """Read frames (writing each to the capture file) until a control message
    of type `wanted` arrives, or ANY control message other than login_ok and
    sub_ack when wanted is None. Ticks arriving meanwhile are recorded too."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - loop.time()))
        except TimeoutError:
            return None
        out.write(json.dumps({"recv": iso_ms(utc_now()), "frame": mask_frame(raw)}) + "\n")
        for msg in split_frame(raw):
            seen.append(msg)
            if message_kind(msg) != "control":
                continue
            mtype = msg.get("type")
            if (wanted is None and mtype not in ("login_ok", "sub_ack")) or mtype == wanted:
                return msg
    return None


def find_strings(obj, tokens: tuple[str, ...], limit: int = 40) -> list[str]:
    """Every string inside a JSON value that contains one of `tokens`. Used to
    pick our candidates out of the (large, undocumented-shape) symbols list."""
    hits: list[str] = []

    def walk(x) -> None:
        if len(hits) >= limit:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                walk(k)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str) and any(t in x.upper() for t in tokens):
            hits.append(x)

    walk(obj)
    return hits


async def capture(
    settings: Settings, ws_symbols: list[str], minutes: float, out_path, list_symbols: bool
) -> dict:
    deadline = time.monotonic() + minutes * 60
    arrivals: dict[str, list[float]] = defaultdict(list)  # "s" value -> monotonic times
    offsets_ms: dict[str, list[float]] = defaultdict(list)  # "s" value -> recv - ts
    control: list[dict] = []
    disconnects: list[str] = []
    acks: list[dict] = []
    symbols_reply: dict | None = None
    frames = 0

    def record(msg: dict, recv) -> None:
        kind = message_kind(msg)
        if kind == "control":
            safe = masked_control(msg)
            control.append(safe)
            print("control:", json.dumps(safe)[:500])
        elif kind in ("quote", "snapshot"):
            sym = msg.get("s", "?")
            if kind == "quote":
                arrivals[sym].append(time.monotonic())
            else:
                arrivals.setdefault(sym, [])  # seen, even if only as a snapshot
            try:
                ts = parse_provider_ts(msg["ts"])
                offsets_ms[sym].append((recv - ts).total_seconds() * 1000)
            except (KeyError, ValueError):
                pass

    with open(out_path, "w") as out:
        while time.monotonic() < deadline:
            try:
                async with connect(
                    settings.ws_url, ping_interval=settings.ws_ping_interval_s
                ) as ws:
                    seen: list[dict] = []
                    await ws.send(
                        json.dumps({"action": "login", "key": settings.ws_key, "fmt": "JSON"})
                    )
                    login = await wait_for_type(ws, "login_ok", 10, out, seen)
                    if login is None:
                        raise TimeoutError("no login_ok within 10 s")
                    # Ask the server what it offers, once, before subscribing,
                    # so the reply cannot be confused with tick traffic.
                    if list_symbols and symbols_reply is None:
                        await ws.send(json.dumps({"action": "symbols"}))
                        symbols_reply = await wait_for_type(ws, None, 15, out, seen) or {}
                    await ws.send(
                        json.dumps(
                            {
                                "action": "subscribe",
                                "send_last": True,
                                "symbols": [f"{s}:QUOTE" for s in ws_symbols],
                            }
                        )
                    )
                    ack = await wait_for_type(ws, "sub_ack", 10, out, seen)
                    if ack is not None:
                        acks.append(ack)
                    now = utc_now()
                    for msg in seen:
                        # The (big) symbols reply is saved to its own file
                        if msg is not symbols_reply:
                            record(msg, now)
                    frames += len(seen)
                    while time.monotonic() < deadline:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except TimeoutError:
                            continue
                        recv = utc_now()
                        frames += 1
                        out.write(
                            json.dumps({"recv": iso_ms(recv), "frame": mask_frame(raw)}) + "\n"
                        )
                        for msg in split_frame(raw):
                            record(msg, recv)
            except (ConnectionClosed, OSError, TimeoutError) as exc:
                reason = f"{datetime.now(UTC):%H:%M:%S} {type(exc).__name__}: {exc}"
                disconnects.append(reason)
                print("DISCONNECT:", reason)
                await asyncio.sleep(2)

    # ---- report -------------------------------------------------------------
    per_symbol = {}
    longest_gap = 0.0
    for sym, times in sorted(arrivals.items()):
        gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
        span = (times[-1] - times[0]) if len(times) > 1 else 0.0
        max_gap = max(gaps) if gaps else None
        longest_gap = max(longest_gap, max_gap or 0.0)
        per_symbol[sym] = {
            "ticks": len(times),
            "ticks_per_s": round(len(times) / span, 2) if span else None,
            "max_gap_s": round(max_gap, 2) if max_gap is not None else None,
            "p99_gap_s": (
                round(statistics.quantiles(gaps, n=100)[98], 3) if len(gaps) >= 2 else None
            ),
            "median_recv_minus_ts_ms": (
                round(statistics.median(offsets_ms[sym]), 1) if offsets_ms[sym] else None
            ),
        }
    last_ack = acks[-1] if acks else {}
    return {
        "captured_minutes": minutes,
        "requested": [f"{s}:QUOTE" for s in ws_symbols],
        # The verification this script exists for: which codes the server took
        "sub_ack": {
            k: last_ack.get(k) for k in ("accepted", "denied", "denied_reasons", "invalid")
        },
        # The "s" values that actually arrived on ticks/snapshots, i.e. what
        # topology.toml's ws_symbol must equal for the normalizer to match
        "tick_symbols_seen": sorted(arrivals),
        "symbols_reply": symbols_reply,
        "frames": frames,
        "control_messages": control,
        "disconnects": disconnects,
        "per_symbol": per_symbol,
        # Rule of thumb: 3x the longest silence seen, never below 30 s
        "suggested_STALE_AFTER_S": max(30, math.ceil(3 * longest_gap)) if longest_gap else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Record raw TraderMade frames")
    parser.add_argument("--minutes", type=float, default=10)
    parser.add_argument(
        "--symbols",
        default="",
        help="comma-separated WebSocket codes to try (default: every ws_symbol in topology.toml)",
    )
    parser.add_argument(
        "--list-symbols",
        action="store_true",
        help='also send {"action":"symbols"} and save the reply to <stamp>.symbols.json',
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    ws_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or [
        i.ws_symbol for i in settings.topology.instruments.values()
    ]
    if not settings.ws_key:
        print("TRADERMADE_WS_KEY is empty - fill in .env", file=sys.stderr)
        return 2
    if producer_running():
        print("The producer is running. Stop it first: one key = one session.", file=sys.stderr)
        return 2

    raw_dir = REPO_ROOT / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_path = raw_dir / f"{stamp}.jsonl"
    print(f"Capturing {args.minutes} min of frames to {out_path} (Ctrl-C stops early)")

    try:
        report = asyncio.run(
            capture(settings, ws_symbols, args.minutes, out_path, args.list_symbols)
        )
    except KeyboardInterrupt:
        print("stopped early")
        return 1
    # The symbols list can be thousands of entries: keep it out of the
    # printed report, save it whole, and print only the entries that look
    # like our candidates.
    reply = report.pop("symbols_reply")
    if reply is not None:
        sym_path = raw_dir / f"{stamp}.symbols.json"
        sym_path.write_text(json.dumps(masked_control(reply), indent=2))
        tokens = tuple({s.split(".")[0].upper()[:4] for s in ws_symbols})
        report["symbols_reply_file"] = str(sym_path.relative_to(REPO_ROOT))
        report["symbols_reply_matches"] = find_strings(reply, tokens)
    summary_path = raw_dir / f"{stamp}.summary.json"
    summary_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print(f"\nreport saved to {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
