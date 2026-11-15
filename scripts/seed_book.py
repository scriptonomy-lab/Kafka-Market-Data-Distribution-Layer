"""
Seed the synthetic risk book's entry prices from TraderMade REST /live.

Why: if entry prices were typed by hand, the Risk screen would show large,
meaningless P&L the moment the market moved away from them. Seeding entries
at the current mid makes P&L start near zero and move with the live market,
which is what a reader expects to see.

What it changes: config/positions.json -> entry_price, seeded_at, seed_source.
Sides and quantities are left exactly as you set them.

REST codes are not our symbols (topology.toml rest_symbol: "AAPL" on REST,
"AAPL.US" on the WebSocket, "AAPL" internally). An instrument with no
rest_symbol (MSFT, NVDA: REST rejected every code tried) is left unseeded and
Risk locks in the first mid it sees, which it logs.

In Docker this runs as the one-shot `seed-book` service, the only container
that receives the REST key:  make seed-book

Run on a weekday while FX is open:  make seed-book
"""

from __future__ import annotations

import json
import sys

from pipeline import rest
from pipeline.common import REPO_ROOT, Settings, iso_ms, utc_now

POSITIONS_FILE = REPO_ROOT / "config" / "positions.json"


def main() -> int:
    settings = Settings.from_env()
    book = json.loads(POSITIONS_FILE.read_text())
    topology = settings.topology
    # internal symbol -> REST code, for the positions REST can price
    rest_code = {
        p["symbol"]: topology.instrument(p["symbol"]).rest_symbol
        for p in book["positions"]
        if topology.instrument(p["symbol"]).rest_symbol
    }

    try:
        quotes = rest.live_quotes(settings, tuple(sorted(set(rest_code.values()))))
    except rest.RestError as exc:
        print(f"REST call failed: {exc}", file=sys.stderr)
        return 1

    for pos in book["positions"]:
        if pos["symbol"] not in rest_code:
            print(f"{pos['symbol']}: no REST code in topology.toml - left unchanged")
            continue
        q = quotes.get(rest_code[pos["symbol"]])
        if q is None or q.get("mid") is None:
            print(f"no REST quote for {pos['symbol']} - left unchanged", file=sys.stderr)
            continue
        pos["entry_price"] = q["mid"]
        print(f"{pos['side']:<5} {pos['quantity']:>12,.0f} {pos['symbol']}  entry = {q['mid']}")

    book["seeded_at"] = iso_ms(utc_now())
    book["seed_source"] = "TraderMade REST /live (mid)"
    POSITIONS_FILE.write_text(json.dumps(book, indent=2) + "\n")
    print(
        f"updated {POSITIONS_FILE.relative_to(REPO_ROOT)} — restart the Risk consumer to pick it up"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
