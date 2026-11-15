"""
Create every topic in config/topology.toml with deliberate settings
(auto-creation is disabled on the broker, so nothing is ever created with
defaults by accident).

Per topic, from topology.toml:
    partitions      chosen with scripts/partition_map.py so each symbol of the
                    topic hashes to its own partition
    retention.ms    per asset class: crypto (24/7) shortest, equities longest
    replication     1 (single broker; a stated limitation)
    cleanup.policy  delete (old segments are deleted, not compacted)

Idempotent: a topic that already exists with the configured partition count
is left alone. A topic that exists with a DIFFERENT partition count is an
error, never "fixed": changing the count changes which partition every
symbol hashes to, which would break per-symbol ordering across the change.
The fix is a deliberate `make reset` (or a new topic name).

In Docker this runs as the one-shot `kafka-init` service; on the host:
    make topics        (or: python -m scripts.create_topics)
"""

from __future__ import annotations

import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.common import Settings


def main() -> int:
    settings = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = admin.list_topics(timeout=15).topics

    failed = False
    to_create = []
    for t in settings.topology.topics.values():
        if t.name in existing:
            n = len(existing[t.name].partitions)
            if n != t.partitions:
                print(
                    f"ERROR {t.name!r} exists with {n} partitions, topology.toml says "
                    f"{t.partitions}. Changing the count re-maps every symbol to a different "
                    f"partition; run `make reset` to start clean.",
                    file=sys.stderr,
                )
                failed = True
            else:
                print(f"ok      {t.name!r} already exists with {n} partitions")
            continue
        to_create.append(
            NewTopic(
                t.name,
                num_partitions=t.partitions,
                replication_factor=1,
                config={"retention.ms": str(t.retention_ms), "cleanup.policy": "delete"},
            )
        )

    if to_create:
        # create_topics is asynchronous: it returns {topic: future}
        futures = admin.create_topics(to_create, request_timeout=15)
        for new in to_create:
            try:
                futures[new.topic].result()
                topic = settings.topology.topic_by_name(new.topic)
                print(
                    f"created {new.topic!r}: {topic.partitions} partitions, replication 1, "
                    f"retention {topic.retention_ms / 86_400_000:.1f} days"
                )
            except KafkaException as exc:
                print(f"ERROR failed to create {new.topic!r}: {exc}", file=sys.stderr)
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
