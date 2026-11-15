"""
Spot-check: are the Pricing consumer's latest prices the same market that
TraderMade's REST /live endpoint reports right now?

Pricing writes run/pricing_state.json every second; this script reads it,
calls REST /live once, and prints both side by side with the difference in
pips (FX) or ticks. REST codes come from topology.toml rest_symbol;
instruments without one are listed but not compared. The two snapshots are taken a moment apart through different channels,
so small differences (a fraction of a pip, or a pip in a fast market) are
expected. Large or persistent ones mean something upstream is wrong (stale
consumer, wrong symbol mapping, timezone bug).

Run while Pricing is running:  make reconcile
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from pipeline import rest
from pipeline.common import Settings


def main() -> int:
    settings = Settings.from_env()
    state_path = settings.run_dir / "pricing_state.json"
    if not state_path.exists():
        print(
            "run/pricing_state.json not found - start the Pricing consumer first", file=sys.stderr
        )
        return 1
    state = json.loads(state_path.read_text())

    topology = settings.topology
    codes = {s: topology.instrument(s).rest_symbol for s in state if s in topology.instruments}
    try:
        live = rest.live_quotes(settings, tuple(sorted({c for c in codes.values() if c})))
    except rest.RestError as exc:
        print(f"REST call failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"{'symbol':<8} {'pricing mid':>12} {'REST mid':>12} {'diff (pip/tick)':>15}  pricing tick time      REST quote time"
    )
    for sym in [s for s in topology.symbols if s in state]:
        ours, theirs = state[sym], live.get(codes.get(sym) or "")
        if not theirs or theirs.get("mid") is None:
            print(f"{sym:<8} {ours['mid']:>12} {'-':>12}   (no REST code or no REST quote)")
            continue
        diff = (ours["mid"] - theirs["mid"]) / topology.instrument(sym).pip_or_tick_size
        rest_time = (
            datetime.fromtimestamp(theirs["timestamp"], UTC).strftime("%H:%M:%S UTC")
            if theirs.get("timestamp")
            else "-"
        )
        print(
            f"{sym:<8} {ours['mid']:>12} {theirs['mid']:>12} {diff:>15.2f}  "
            f"{ours['timestamp'][11:23]:<22} {rest_time}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
