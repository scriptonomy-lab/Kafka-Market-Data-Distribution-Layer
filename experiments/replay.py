"""
Experiment D — replay: a brand-new consumer group rebuilds history from the
log, across all three topics.

The idea in one sentence: Kafka does not delete a message when someone reads
it, so a system that did not exist yesterday (a new analytics job, an audit,
a fixed version of a buggy consumer) can read everything still retained and
arrive at the same data as the consumers that saw it live.

What this script does
    1. Records, for every topic-partition of the three topics, the current
       end offset (the replay target) and low watermark (oldest retained).
    2. Reads everything in between with a NEW group, "replay-audit-<stamp>".
    3. Checks offsets were contiguous (no holes), counts events per topic and
       symbol, measures replay throughput.
    4. Compares, per topic-partition, with what Storage holds in TimescaleDB
       for the offsets Storage has COMMITTED (it may be a few seconds behind
       the replay target, or stopped). Within that range the replay read
       every offset exactly once, so the expected count is simply
       committed - low per partition. Retention differs per topic (crypto
       3 days, FX 7, equities 14), so each topic's low watermark is its own.

Output: results/<stamp>-expD-replay/summary.json and summary.md
        (results/_dev/... if any replayed event came from the simulator)

Run:  make replay
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from experiments import db_checks
from pipeline.common import (
    GROUP_STORAGE,
    REPO_ROOT,
    Settings,
    consumer_config,
    sample_groups,
    topic_partitions,
    write_json_atomic,
)

GIVE_UP_S = 600


def main() -> int:
    settings = Settings.from_env()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    group = f"replay-audit-{stamp}"
    topics = settings.topology.topic_names
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    tps = topic_partitions(admin, topics)

    consumer = Consumer(
        consumer_config(settings, group, auto_commit=False, offset_reset="earliest")
    )
    # Target = end offsets NOW; messages produced while replaying are excluded
    low, target = {}, {}
    for t, p in tps:
        lo, hi = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10, cached=False)
        low[(t, p)], target[(t, p)] = lo, hi
    expected = sum(target[tp] - low[tp] for tp in tps)
    print(f"replaying {expected} retained messages from {len(topics)} topics with group {group!r}")

    # assign() instead of subscribe(): we want every partition, from the
    # oldest retained offset, without waiting for a group rebalance.
    consumer.assign([TopicPartition(t, p, low[(t, p)]) for t, p in tps])

    next_expected = dict(low)  # per topic-partition: the offset we expect next
    holes = 0
    per_symbol: Counter[str] = Counter()
    per_topic: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    read = 0
    done = {tp for tp in tps if target[tp] <= low[tp]}  # empty partitions
    t0 = time.monotonic()
    while len(done) < len(tps):
        for m in consumer.consume(num_messages=1000, timeout=2.0):
            if m.error():
                continue
            tp, off = (m.topic(), m.partition()), m.offset()
            if tp in done or off >= target[tp]:
                continue
            if off != next_expected[tp]:
                holes += 1  # an offset was skipped: should never happen
            next_expected[tp] = off + 1
            e = json.loads(m.value())
            per_symbol[e["symbol"]] += 1
            per_topic[tp[0]] += 1
            sources[e.get("source", "unknown")] += 1
            read += 1
            if next_expected[tp] >= target[tp]:
                done.add(tp)
        if time.monotonic() - t0 > GIVE_UP_S:
            print(f"gave up after {GIVE_UP_S} s", file=sys.stderr)
            break
    elapsed = time.monotonic() - t0

    # Compare with Storage over the offsets it has COMMITTED.
    storage = sample_groups(admin, consumer, [GROUP_STORAGE], tps)[0]
    consumer.close()
    cut = {tp: min(target[tp], max(storage.committed.get(tp, -1), low[tp])) for tp in tps}
    stored = None
    compare: dict[str, dict] = {}
    try:
        with db_checks.connect(settings) as conn:
            per_tp = db_checks.stored_per_partition(conn, low, cut)
        stored = sum(d["stored"] for d in per_tp.values())
        for (t, _p), d in per_tp.items():
            c = compare.setdefault(t, {"replayed_in_range": 0, "stored_in_range": 0})
            c["replayed_in_range"] += d["expected"]  # replay had 0 holes in it
            c["stored_in_range"] += d["stored"]
        for c in compare.values():
            c["match"] = c["replayed_in_range"] == c["stored_in_range"]
    except Exception as exc:  # report, do not hide: the replay itself still counts
        print(f"database comparison unavailable: {type(exc).__name__}", file=sys.stderr)
    replayed_in_cut = sum(cut[tp] - low[tp] for tp in tps)

    summary = {
        "group": group,
        "topics": topics,
        "retained_range": {f"{t}:{p}": [low[(t, p)], target[(t, p)]] for t, p in tps},
        "expected": expected,
        "replayed": read,
        "offset_holes": holes,
        "per_topic": dict(per_topic),
        "per_symbol": dict(sorted(per_symbol.items())),
        "sources": dict(sources),
        "seconds": round(elapsed, 2),
        "replay_rate_msgs_per_s": round(read / elapsed, 1) if elapsed else None,
        "compared_range": {f"{t}:{p}": [low[(t, p)], cut[(t, p)]] for t, p in tps},
        "replayed_in_compared_range": replayed_in_cut,
        "storage_rows_same_range": stored,
        "comparison_per_topic": compare,
        "matches_storage": stored == replayed_in_cut if stored is not None else None,
        "simulated_feed": "simulated" in sources,
    }
    dev = "_dev" if summary["simulated_feed"] else ""
    out = settings.results_dir / dev / f"{stamp}-expD-replay"
    out.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out / "summary.json", summary)
    (out / "summary.md").write_text(render_markdown(stamp, summary))
    print(
        json.dumps(
            {
                k: summary[k]
                for k in ("expected", "replayed", "offset_holes", "per_topic", "matches_storage")
            },
            indent=2,
        )
    )
    print(f"written to {out.relative_to(REPO_ROOT)}")
    ok = holes == 0 and read == expected and summary["matches_storage"] is not False
    return 0 if ok else 1


def render_markdown(stamp: str, s: dict) -> str:
    """Human-readable summary; every number comes from summary.json."""
    return "\n".join(
        [
            f"# Experiment D (replay) — {stamp}",
            "",
            "**SIMULATED FEED — NOT EVIDENCE**"
            if s["simulated_feed"]
            else "Source: live TraderMade feed.",
            "",
            "| Measure | Value |",
            "| --- | --- |",
            f"| New consumer group | `{s['group']}` |",
            f"| Retained messages, all topics (expected) | {s['expected']} |",
            f"| Messages replayed | {s['replayed']} |",
            f"| Offset holes | {s['offset_holes']} |",
            f"| Replay time (s) | {s['seconds']} |",
            f"| Replay rate (msg/s) | {s['replay_rate_msgs_per_s']} |",
            f"| Compared with Storage up to its committed offsets | {s['replayed_in_compared_range']} replayed vs {s['storage_rows_same_range']} stored |",
            f"| Replay matches Storage | {s['matches_storage']} |",
            "",
            "| Topic | Replayed | Replayed in compared range | Stored in TimescaleDB | Match |",
            "| --- | --- | --- | --- | --- |",
            *[
                f"| {t} | {s['per_topic'].get(t, 0)} | {c['replayed_in_range']} | {c['stored_in_range']} | {c['match']} |"
                for t, c in s["comparison_per_topic"].items()
            ],
            "",
            "| Symbol | Events |",
            "| --- | --- |",
            *[f"| {sym} | {n} |" for sym, n in s["per_symbol"].items()],
            "",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
