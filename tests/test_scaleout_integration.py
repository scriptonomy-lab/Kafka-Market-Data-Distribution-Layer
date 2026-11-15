"""
Integration tests for Storage scale-out, against the REAL broker from docker
compose (make test-integration). They use throw-away topics and groups.

    1. Two members of one group split the partitions of all three topics and
       rebalance cooperatively on join and leave, and every member owns at
       least one partition that carries data (scripts/assignment_check.py,
       one trial of experiment F's membership sequence).
    2. No message is lost when one of two members dies (SIGKILL): the
       survivor takes over its partitions after session.timeout.ms and
       resumes from the dead member's last COMMITTED offsets. Messages the
       dead member processed but had not committed are redelivered
       (duplicates), never skipped.
"""

import json
import signal
import subprocess
import sys
import time
import uuid

import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.common import REPO_ROOT, Settings, consumer_config, producer_config

pytestmark = pytest.mark.integration
SETTINGS = Settings.from_env()
KEYS = ["EURUSD", "GBPUSD", "USDJPY", "BTCUSD", "ETHUSD", "AAPL"]


@pytest.fixture(scope="module")
def admin():
    a = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap})
    try:
        a.list_topics(timeout=3)
    except Exception:
        pytest.skip(f"no Kafka broker at {SETTINGS.kafka_bootstrap}")
    return a


def test_members_split_and_rebalance_cooperatively_and_all_own_data(admin):
    from scripts.assignment_check import run_trial

    result = run_trial(SETTINGS, admin, trial=1)
    for step in result["steps"]:
        assert step["every_member_has_data"], step
    assert result["steps"][1]["only_moved_partitions_revoked"]
    two, three, after_leave = (s["owned"] for s in result["steps"])
    assert sorted(two.values()) == [9, 9]
    assert sorted(three.values()) == [6, 6, 6]
    assert sorted(after_leave.values()) == [9, 9]


def produce(topic, n_per_key, start):
    p = Producer(producer_config(SETTINGS))
    for i in range(start, start + n_per_key):
        for k in KEYS:
            p.produce(topic, key=k.encode(), value=json.dumps({"k": k, "i": i}).encode())
    assert p.flush(15) == 0


def test_no_message_lost_when_one_of_two_members_is_killed(admin, tmp_path):
    topic = f"it-scaleout-{uuid.uuid4().hex[:8]}"
    group = f"it-scaleout-{uuid.uuid4().hex[:6]}"
    admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)])[topic].result()
    out_b = tmp_path / "b.txt"
    seen_a: list[tuple[int, int]] = []
    b = None
    try:
        produce(topic, 100, 0)
        a = Consumer(
            consumer_config(
                SETTINGS,
                group,
                auto_commit=False,
                offset_reset="earliest",
                **{"enable.auto.offset.store": False, "client.id": "it-member-a"},
            )
        )
        a.subscribe([topic])
        b = subprocess.Popen(
            [sys.executable, "-m", "tests._member", topic, group, str(out_b)], cwd=REPO_ROOT
        )

        def step_a(timeout=0.5):
            msgs = [m for m in a.consume(num_messages=100, timeout=timeout) if not m.error()]
            for m in msgs:
                seen_a.append((m.partition(), m.offset()))
                a.store_offsets(message=m)
            if msgs:
                try:
                    a.commit(asynchronous=False)
                except KafkaException:
                    # Commits fail while a rebalance is in progress (e.g.
                    # ILLEGAL_GENERATION); the stored offsets go out with the
                    # next commit. Storage handles this the same way.
                    pass

        # Keep traffic flowing until B has joined, owns partitions and has
        # processed something, then kill it. (Without new traffic A would
        # have read everything before B even joined.)
        deadline, i = time.monotonic() + 60, 1000
        while time.monotonic() < deadline:
            produce(topic, 5, i)
            i += 5
            step_a()
            if out_b.exists() and out_b.read_text().strip():
                break
        assert out_b.read_text().strip(), "member B never received messages"
        produce(topic, 100, 100)  # more traffic while B is alive
        for _ in range(4):
            step_a()
        b.send_signal(signal.SIGKILL)  # crash: no commit, no LeaveGroup
        b.wait(timeout=10)
        produce(topic, 100, 200)  # traffic after the crash

        # A must end up with everything B left behind, after session.timeout.ms
        ends = {}
        for p in range(6):
            from confluent_kafka import TopicPartition

            ends[p] = a.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
        expected = {(p, o) for p, e in ends.items() for o in range(e)}
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            step_a()
            seen_b = {tuple(map(int, line.split())) for line in out_b.read_text().splitlines()}
            if expected <= set(seen_a) | seen_b:
                break
        a.close()
        seen_b = {tuple(map(int, line.split())) for line in out_b.read_text().splitlines()}
        missing = expected - set(seen_a) - seen_b
        assert not missing, f"{len(missing)} messages lost, e.g. {sorted(missing)[:5]}"
        # Redelivery is allowed (at-least-once); report it for the curious
        print(f"duplicates after crash: {len(seen_a) + len(seen_b) - len(expected)}")
    finally:
        if b is not None and b.poll() is None:
            b.kill()
        admin.delete_topics([topic])[topic].result()
