"""
DEV ONLY — an offline stand-in for TraderMade WebSocket V2.

Why it exists
    FX is closed from Friday 17:00 to Sunday 17:00 New York time, and you
    should not burn your real session for every code change. This simulator
    speaks the same protocol subset the producer uses, so you can build and
    debug the whole pipeline on a weekend with the SAME producer code.

Why it can never contaminate evidence
    The producer tags every event with source="simulated" whenever
    TRADERMADE_WS_URL is not the official host, Storage writes that tag into
    every row, and the experiment runner refuses to treat simulated runs as
    evidence (it writes them under results/_dev/).

Protocol subset implemented (mirrors the V2 docs)
    login        -> login_ok, or login_reject/unknown_key for the key "bad"
                    a second login with the same key logs the first session
                    out with reason "duplicate_login", like the real server
    subscribe    -> sub_ack (accepted / invalid), then optional LAST_QUOTE
                    per symbol when send_last is true, then QUOTE ticks
    unsubscribe  -> unsub_ack
    symbols      -> {"type":"symbols","symbols":[...]} (the codes it knows)
    anything else-> {"type":"error","reason":"unknown_action"}

Codes mirror the ones verified against the live service (topology.toml):
FX pairs, BTCUSD/ETHUSD, and US stocks as "AAPL.US"-style codes. Bare tickers
("AAPL") are invalid, as on the real server. Stocks tick around the clock
here; the real feed only ticks them in US market hours.
    bad JSON     -> {"type":"error","reason":"invalid_json"}
Ticks are a random walk with prices sent as STRINGS, exactly like the feed.

Usage (two terminals):
    make fake-feed                                   # or ARGS="--rate 20 --drop-every 30"
    TRADERMADE_WS_URL=ws://localhost:8765/feedAdv make producer
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

# Starting mids for the random walk. Deliberately round numbers: obviously fake.
START_MIDS = {
    "EURUSD": 1.16,
    "GBPUSD": 1.33,
    "USDJPY": 152.0,
    "AUDUSD": 0.66,
    "BTCUSD": 100_000.0,
    "ETHUSD": 3_500.0,
    "AAPL.US": 230.0,
    "MSFT.US": 420.0,
    "NVDA.US": 180.0,
}
SPREAD_PIPS = 0.3  # FX spread in pips; other instruments use SPREAD_UNITS
# Price step ("pip") and display decimals for non-FX codes
UNIT = {"BTCUSD": 1.0, "ETHUSD": 0.1, "AAPL.US": 0.01, "MSFT.US": 0.01, "NVDA.US": 0.01}
SPREAD_UNITS = {"BTCUSD": 10, "ETHUSD": 5, "AAPL.US": 2, "MSFT.US": 2, "NVDA.US": 2}


@dataclass
class SimConfig:
    rate: float = 5.0  # ticks per second per subscribed symbol
    drop_every: float = 0.0  # close each connection after N seconds (0 = never)
    symbol_limit: int = 10  # reported in login_ok, like the real plan
    seed: int | None = None


@dataclass
class SimState:
    mids: dict = field(default_factory=lambda: dict(START_MIDS))
    sessions: dict = field(default_factory=dict)  # key -> websocket


def provider_ts(now: datetime | None = None) -> str:
    """Format like TraderMade's "ts": YYYYMMDD-HH:MM:SS.sss (UTC)."""
    now = now or datetime.now(UTC)
    return now.strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def pip(symbol: str) -> float:
    """Random-walk step size: one FX pip, or the instrument's unit."""
    if symbol in UNIT:
        return UNIT[symbol]
    return 0.01 if symbol.endswith("JPY") else 0.0001


def make_quote(symbol: str, mid: float, kind: str = "QUOTE") -> dict:
    half = SPREAD_UNITS.get(symbol, SPREAD_PIPS) * pip(symbol) / 2
    digits = 2 if symbol in UNIT else (3 if symbol.endswith("JPY") else 5)
    msg = {
        "a": f"{mid + half:.{digits + 4}f}",  # strings, like the real feed
        "b": f"{mid - half:.{digits + 4}f}",
        "s": symbol,
        "t": kind,
        "ts": provider_ts(),
    }
    if kind == "QUOTE":
        msg["av"], msg["bv"] = "100000", "100000"
    else:
        msg["m"] = f"{mid:.{digits + 4}f}"  # LAST_QUOTE carries a mid
    return msg


async def stream_ticks(ws, subscribed: set, cfg: SimConfig, state: SimState) -> None:
    """Send random-walk QUOTE ticks for the subscribed symbols until closed."""
    started = asyncio.get_running_loop().time()
    while True:
        if not subscribed:
            await asyncio.sleep(0.1)
            continue
        # Total rate across symbols; pick a symbol at random for each tick
        await asyncio.sleep(random.expovariate(cfg.rate * len(subscribed)))
        symbol = random.choice(sorted(subscribed))
        state.mids[symbol] += random.gauss(0, 0.2) * pip(symbol)
        await ws.send(json.dumps(make_quote(symbol, state.mids[symbol])))
        if cfg.drop_every and asyncio.get_running_loop().time() - started > cfg.drop_every:
            # Simulate a network/server drop: close without a logout message
            await ws.close(code=1012, reason="simulated drop")
            return


async def handle_connection(ws, cfg: SimConfig, state: SimState) -> None:
    key = None
    subscribed: set[str] = set()
    streamer: asyncio.Task | None = None
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps({"type": "error", "reason": "invalid_json"}))
                continue
            action = msg.get("action")

            if action == "login":
                if msg.get("key") in (None, "", "bad"):
                    reply = {"type": "login_reject", "key": msg.get("key"), "reason": "unknown_key"}
                    await ws.send(json.dumps(reply))
                    await ws.close()
                    return
                key = msg["key"]
                old = state.sessions.get(key)
                if old is not None and old is not ws:
                    # One session per key, exactly like the real server
                    try:
                        await old.send(
                            json.dumps({"type": "logout", "key": key, "reason": "duplicate_login"})
                        )
                        await old.close()
                    except ConnectionClosed:
                        pass
                state.sessions[key] = ws
                await ws.send(
                    json.dumps(
                        {
                            "type": "login_ok",
                            "key": key,
                            "fmt": "JSON",
                            "symbol_limit": cfg.symbol_limit,
                            "cfds": False,
                            "trader_ladder": False,
                        }
                    )
                )

            elif action == "subscribe":
                if key is None:
                    await ws.send(json.dumps({"type": "error", "reason": "not_logged_in"}))
                    continue
                accepted, invalid = [], []
                for item in msg.get("symbols", []):
                    # rpartition: "AAPL.US:QUOTE" -> "AAPL.US"
                    sym = item.rpartition(":")[0] or item
                    (accepted if sym in state.mids else invalid).append(item)
                    if sym in state.mids:
                        subscribed.add(sym)
                await ws.send(
                    json.dumps(
                        {
                            "accepted": accepted,
                            "denied": [],
                            "denied_reasons": {},
                            "invalid": invalid,
                            "type": "sub_ack",
                        }
                    )
                )
                if msg.get("send_last"):
                    for item in accepted:
                        sym = item.rpartition(":")[0] or item
                        await ws.send(json.dumps(make_quote(sym, state.mids[sym], "LAST_QUOTE")))
                if streamer is None:
                    streamer = asyncio.create_task(stream_ticks(ws, subscribed, cfg, state))

            elif action == "symbols":
                await ws.send(json.dumps({"type": "symbols", "symbols": sorted(state.mids)}))

            elif action == "unsubscribe":
                removed = [i for i in msg.get("symbols", []) if i.rpartition(":")[0] in subscribed]
                for i in removed:
                    subscribed.discard(i.rpartition(":")[0])
                invalid = [i for i in msg.get("symbols", []) if i not in removed]
                await ws.send(
                    json.dumps({"type": "unsub_ack", "removed": removed, "invalid": invalid})
                )

            else:
                await ws.send(
                    json.dumps({"type": "error", "reason": "unknown_action", "action": action})
                )
    except ConnectionClosed:
        pass
    finally:
        if streamer is not None:
            streamer.cancel()
        if key is not None and state.sessions.get(key) is ws:
            del state.sessions[key]


async def start_server(host: str, port: int, cfg: SimConfig):
    """Start the simulator and return the server object (used by tests)."""
    if cfg.seed is not None:
        random.seed(cfg.seed)
    state = SimState()
    return await serve(lambda ws: handle_connection(ws, cfg, state), host, port)


async def _amain(args) -> None:
    cfg = SimConfig(rate=args.rate, drop_every=args.drop_every, seed=args.seed)
    server = await start_server(args.host, args.port, cfg)
    print(
        f"DEV simulator on ws://{args.host}:{args.port}/feedAdv  rate={cfg.rate}/s/symbol  drop_every={cfg.drop_every}s"
    )
    print("Point the producer at it:  TRADERMADE_WS_URL=ws://localhost:8765/feedAdv make producer")
    await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="DEV ONLY TraderMade WebSocket V2 simulator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--rate", type=float, default=5.0, help="ticks/s per symbol")
    parser.add_argument(
        "--drop-every", type=float, default=0.0, help="drop each connection after N s"
    )
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
