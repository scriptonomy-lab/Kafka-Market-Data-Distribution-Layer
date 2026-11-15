"""
Does every Storage instance own at least one partition that carries data?

Why this exists
---------------
The partition map (scripts/partition_map.py) says WHERE each symbol lives: 8
of the 18 partitions carry data, 10 stay empty. It cannot say WHO owns them:
that is decided by the group's assignor (cooperative-sticky) and depends on
which members joined or left, in what order. An instance that owned only
empty partitions would be "scaled out" and still do nothing, so experiment F
would show nothing. This script checks it against the REAL broker instead of
assuming it.

What it does, per trial
-----------------------
Creates three throw-away topics with the same partition counts as
topology.toml (names keep the real ones as a prefix, so they sort the same
way), then walks experiment F's membership sequence with real consumers that
use the Storage settings (pipeline/common.py:consumer_config):

    2 members  ->  3 members  ->  one leaves cleanly (2 left)

After every step it waits for a settled assignment and records which
partitions each member owns, and whether each owns >= 1 non-empty partition
(non-empty = a symbol hashes there, from the murmur2 map). It also checks
the cooperative property: a member's partitions that were not handed to the
newcomer were never revoked.

    make assignment-check                 5 trials
    make assignment-check ARGS="--trials 20 --json results/_dev/assign.json"
    make assignment-check ARGS=--save   writes results/<stamp>-assignment-check/
                                        (summary.json + summary.md) for citing

A crash (SIGKILL) step is not simulated here: when a member dies, the
survivors KEEP their partitions (sticky) and receive the dead member's, so a
survivor that owned a non-empty partition before still owns one after.
Experiment F measures the real crash.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.common import Settings, consumer_config
from pipeline.topology import expected_partition_map

SETTLE_STABLE_S = 3.0  # an assignment must stay unchanged this long
SETTLE_TIMEOUT_S = 60.0


def create_topics(admin, settings: Settings, suffix: str) -> dict[str, str]:
    """real topic name -> throw-away topic name, same partition count."""
    names = {t.name: f"{t.name}-check-{suffix}" for t in settings.topology.topics.values()}
    new = [
        NewTopic(names[t.name], num_partitions=t.partitions, replication_factor=1)
        for t in settings.topology.topics.values()
    ]
    for fut in admin.create_topics(new, request_timeout=15).values():
        fut.result()
    return names


def delete_topics(admin, names) -> None:
    for fut in admin.delete_topics(list(names), request_timeout=15).values():
        try:
            fut.result()
        except Exception:
            pass


def new_member(settings: Settings, group: str, name: str, topics: list[str], revoked: dict):
    """A consumer configured exactly like Storage, recording what it loses."""
    cfg = consumer_config(
        settings,
        group,
        auto_commit=False,
        offset_reset="earliest",
        **{"enable.auto.offset.store": False, "client.id": f"check-{name}"},
    )
    c = Consumer(cfg)
    revoked[name] = set()

    def on_revoke(_c, parts):
        revoked[name].update((tp.topic, tp.partition) for tp in parts)

    c.subscribe(topics, on_revoke=on_revoke)
    return c


def settle(members: dict, total: int) -> dict[str, set]:
    """Poll every member until all `total` partitions are owned exactly once
    and nothing changed for SETTLE_STABLE_S. Returns name -> owned set."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    last, since = None, time.monotonic()
    while time.monotonic() < deadline:
        for c in members.values():
            c.poll(0.1)  # serves rebalance callbacks; topics are empty
        owned = {n: {(tp.topic, tp.partition) for tp in c.assignment()} for n, c in members.items()}
        flat = [tp for s in owned.values() for tp in s]
        snapshot = {n: sorted(s) for n, s in owned.items()}
        if snapshot != last:
            last, since = snapshot, time.monotonic()
        elif len(flat) == total == len(set(flat)) and time.monotonic() - since >= SETTLE_STABLE_S:
            return owned
    raise TimeoutError(f"assignment did not settle: {last}")


def run_trial(settings: Settings, admin, trial: int) -> dict:
    suffix = uuid.uuid4().hex[:8]
    names = create_topics(admin, settings, suffix)
    expected = expected_partition_map(settings.topology)
    nonempty = {(names[t], p) for t, syms in expected.items() for p in syms.values()}
    total = sum(t.partitions for t in settings.topology.topics.values())
    group = f"assignment-check-{suffix}"
    topics = list(names.values())
    revoked: dict[str, set] = {}
    members: dict = {}
    steps = []

    def record(step: str, owned: dict) -> None:
        steps.append(
            {
                "step": step,
                "owned": {n: len(s) for n, s in owned.items()},
                "nonempty_owned": {n: len(s & nonempty) for n, s in owned.items()},
                "every_member_has_data": all(s & nonempty for s in owned.values()),
            }
        )

    try:
        members["a"] = new_member(settings, group, "a", topics, revoked)
        members["b"] = new_member(settings, group, "b", topics, revoked)
        before = settle(members, total)
        record("2 members", before)

        members["c"] = new_member(settings, group, "c", topics, revoked)
        for r in revoked.values():
            r.clear()
        after = settle(members, total)
        record("3 members (c joined)", after)
        # Cooperative: what a or b lost is exactly what c received from them
        moved_ok = all(revoked[n] == before[n] - after[n] for n in ("a", "b"))
        steps[-1]["only_moved_partitions_revoked"] = moved_ok

        members.pop("a").close()  # clean leave, as `docker stop` does
        record("2 members (a left)", settle(members, total))
    finally:
        for c in members.values():
            c.close()
        delete_topics(admin, topics)
    ok = all(s["every_member_has_data"] for s in steps) and moved_ok
    return {"trial": trial, "group": group, "steps": steps, "ok": ok}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--json", default="", help="also write the results to this file")
    ap.add_argument(
        "--save", action="store_true", help="write results/<stamp>-assignment-check/ for citing"
    )
    args = ap.parse_args()
    settings = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})

    results = []
    for i in range(1, args.trials + 1):
        r = run_trial(settings, admin, i)
        results.append(r)
        for s in r["steps"]:
            print(
                f"trial {i}  {s['step']:<22} partitions {s['owned']}  "
                f"non-empty {s['nonempty_owned']}  every member has data: {s['every_member_has_data']}"
                + (
                    f"  only moved partitions revoked: {s['only_moved_partitions_revoked']}"
                    if "only_moved_partitions_revoked" in s
                    else ""
                )
            )
    passed = sum(r["ok"] for r in results)
    print(f"\n{passed} of {len(results)} trials passed")
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"trials": results, "passed": passed}, f, indent=2)
    if args.save:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        out = settings.results_dir / f"{stamp}-assignment-check"
        out.mkdir(parents=True, exist_ok=True)
        summary = {
            "run_id": out.name,
            "kafka_bootstrap": settings.kafka_bootstrap,
            "partitions": {t.name: t.partitions for t in settings.topology.topics.values()},
            "assignment_strategy": "cooperative-sticky",
            "trials": results,
            "passed": passed,
            "total": len(results),
            # Not market data: a broker behaviour check on throw-away topics.
            "simulated_feed": None,
        }
        (out / "summary.json").write_text(json.dumps(summary, indent=2))
        lines = [
            f"# Assignment check — {out.name}",
            "",
            "Kafka's cooperative-sticky assignor on throw-away topics with topology.toml's",
            "partition counts; Storage's consumer settings. Not market data.",
            "",
            f"**{passed} of {len(results)} trials passed.**",
            "",
            "| Trial | Step | Partitions per member | With data per member | Every member has data |",
            "| --- | --- | --- | --- | --- |",
            *[
                f"| {r['trial']} | {st['step']} | {st['owned']} | {st['nonempty_owned']} | {st['every_member_has_data']} |"
                for r in results
                for st in r["steps"]
            ],
            "",
        ]
        (out / "summary.md").write_text("\n".join(lines))
        print(f"written to {out}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
