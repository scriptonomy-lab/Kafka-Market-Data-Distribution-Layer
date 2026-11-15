"""
The symbol -> partition map, computed offline and checked against reality.

Why this exists
---------------
With key = symbol, Kafka's partitioner decides where each symbol lives:
partition = toPositive(murmur2(key)) % partitions. Nothing guarantees that
two symbols of a topic land on different partitions (in v1, with 3
partitions, EURUSD and GBPUSD shared partition 2). Two symbols on one
partition cannot be processed in parallel by two consumer instances, so the
partition COUNT is a design decision, and this script is how it was made.

    make partition-map                  print the map for topology.toml
    make partition-map ARGS=--scan      for each topic, show counts 1..12 and
                                        which give every symbol its own partition
    make partition-map ARGS=--check     compare with run/partition_map.json,
                                        the map the producer OBSERVED from
                                        Kafka's delivery reports (exit 1 on
                                        any difference)

The hash is pipeline/topology.py:murmur2, a port of the Java client's
Utils.murmur2; tests/test_partition_map.py pins it to v1's live map.
"""

from __future__ import annotations

import argparse
import json
import sys

from pipeline.common import Settings
from pipeline.topology import expected_partition_map, partition_for


def print_map(expected: dict, partitions: dict[str, int]) -> None:
    for topic, symbols in expected.items():
        used = set(symbols.values())
        empty = [p for p in range(partitions[topic]) if p not in used]
        distinct = len(used) == len(symbols)
        print(f"{topic}  ({partitions[topic]} partitions)")
        for sym, p in symbols.items():
            print(f"    {sym:<8} -> partition {p}")
        print(f"    empty partitions: {empty}  ·  every symbol on its own partition: {distinct}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compute / check the symbol -> partition map")
    ap.add_argument("--scan", action="store_true", help="try partition counts 1..12 per topic")
    ap.add_argument("--check", action="store_true", help="compare with run/partition_map.json")
    args = ap.parse_args()

    settings = Settings.from_env()
    topology = settings.topology
    expected = expected_partition_map(topology)
    partitions = {t.name: t.partitions for t in topology.topics.values()}

    if args.scan:
        for t in topology.topics.values():
            print(f"{t.name}  symbols {list(t.symbols)}")
            for n in range(1, 13):
                parts = [partition_for(s, n) for s in t.symbols]
                ok = "distinct" if len(set(parts)) == len(parts) else ""
                print(f"    {n:>2} partitions: {parts}  {ok}")
        return 0

    print_map(expected, partitions)
    if not args.check:
        return 0

    observed_path = settings.run_dir / "partition_map.json"
    if not observed_path.exists():
        print(f"\n{observed_path} not found: run the producer first", file=sys.stderr)
        return 1
    observed = json.loads(observed_path.read_text())
    diffs = []
    for topic, symbols in expected.items():
        for sym, p in symbols.items():
            got = observed.get(topic, {}).get(sym)
            if got is None:
                diffs.append(f"{topic} {sym}: expected {p}, not yet observed (no tick delivered)")
            elif got != p:
                diffs.append(f"{topic} {sym}: expected {p}, observed {got}")
    print("\nobserved (run/partition_map.json):")
    print(json.dumps(observed, indent=2, sort_keys=True))
    if diffs:
        print("\nDIFFERENCES:\n  " + "\n  ".join(diffs))
        return 1
    print("\nMATCH: every symbol is on the partition the murmur2 computation predicts.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
