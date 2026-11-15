"""
A consumer-group member that runs in its OWN process, so a test can SIGKILL
it (tests/test_scaleout_integration.py). It behaves like Storage in the one
way that matters for the test: it records each message durably (a line in a
file, flushed and fsynced) BEFORE storing and committing its offset.

    python -m tests._member <topic> <group> <out_file>
"""

import os
import sys

from confluent_kafka import Consumer, KafkaException

from pipeline.common import Settings, consumer_config


def main() -> int:
    topic, group, out_path = sys.argv[1:4]
    settings = Settings.from_env()
    c = Consumer(
        consumer_config(
            settings,
            group,
            auto_commit=False,
            offset_reset="earliest",
            **{"enable.auto.offset.store": False, "client.id": "it-member-subprocess"},
        )
    )
    c.subscribe([topic])
    with open(out_path, "a") as out:
        while True:  # runs until killed
            msgs = [m for m in c.consume(num_messages=50, timeout=0.5) if not m.error()]
            if not msgs:
                continue
            for m in msgs:
                out.write(f"{m.partition()} {m.offset()}\n")
            out.flush()
            os.fsync(out.fileno())  # "written" before "marked done"
            for m in msgs:
                c.store_offsets(message=m)
            try:
                c.commit(asynchronous=False)
            except KafkaException:
                pass  # rebalance in progress; stored offsets go with the next commit


if __name__ == "__main__":
    sys.exit(main())
