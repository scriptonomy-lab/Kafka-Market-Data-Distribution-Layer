"""
Integration tests: the Kafka guarantees the article relies on, checked
against a REAL broker at KAFKA_BOOTSTRAP (start one with `make up`).

    make test-integration

Each test uses its own throw-away topic, so it never touches the live
quotes topic or the services' consumer groups. They prove, with the exact
client settings the services use:

    1. keyed messages: same key -> same partition, order kept per key
    2. fan-out: two consumer groups each receive every message
    3. resume: a group restarts exactly at its committed offset
"""

import json
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.common import Settings, consumer_config, producer_config

pytestmark = pytest.mark.integration
SETTINGS = Settings.from_env()


def broker_available() -> bool:
    try:
        AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap}).list_topics(timeout=3)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def topic():
    if not broker_available():
        pytest.skip(f"no Kafka broker at {SETTINGS.kafka_bootstrap}")
    name = f"it-quotes-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result()
    yield name
    admin.delete_topics([name])[name].result()


def produce(topic, n_per_symbol=50):
    p = Producer(producer_config(SETTINGS))
    symbols = ["EURUSD", "GBPUSD", "USDJPY"]
    for i in range(1, n_per_symbol + 1):
        for s in symbols:
            p.produce(topic, key=s.encode(), value=json.dumps({"symbol": s, "seq": i}).encode())
    assert p.flush(15) == 0
    return symbols, n_per_symbol


def drain(consumer, expected, timeout_s=30):
    out = []
    for _ in range(int(timeout_s)):
        for m in consumer.consume(num_messages=1000, timeout=1.0):
            if not m.error():
                out.append(m)
        if len(out) >= expected:
            break
    return out


def new_consumer(group, **overrides):
    return Consumer(
        consumer_config(SETTINGS, group, auto_commit=False, offset_reset="earliest", **overrides)
    )


def test_keyed_messages_keep_per_symbol_order_on_one_partition(topic):
    symbols, n = produce(topic)
    c = new_consumer(f"it-order-{uuid.uuid4().hex[:6]}")
    c.subscribe([topic])
    msgs = drain(c, len(symbols) * n)
    c.close()
    partitions, seqs = {}, {}
    for m in msgs:
        e = json.loads(m.value())
        partitions.setdefault(e["symbol"], set()).add(m.partition())
        seqs.setdefault(e["symbol"], []).append(e["seq"])
    for s in symbols:
        assert len(partitions[s]) == 1, f"{s} spread over partitions {partitions[s]}"
        assert seqs[s] == sorted(seqs[s]), f"{s} out of order"


def test_two_groups_each_receive_every_message(topic):
    # How many messages exist right now (sum of end offsets over 3 partitions)
    c_probe = new_consumer("it-probe")
    end = sum(
        c_probe.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1] for p in range(3)
    )
    c_probe.close()
    counts = {}
    for g in (f"it-a-{uuid.uuid4().hex[:6]}", f"it-b-{uuid.uuid4().hex[:6]}"):
        c = new_consumer(g)
        c.subscribe([topic])
        counts[g] = len(drain(c, end))
        c.close()
    assert set(counts.values()) == {end}, counts


def test_group_resumes_at_committed_offset(topic):
    group = f"it-resume-{uuid.uuid4().hex[:6]}"
    # First run: read some messages, store + commit exactly what we "processed"
    c = new_consumer(group, **{"enable.auto.offset.store": False})
    c.subscribe([topic])
    first = drain(c, 30)[:30]
    committed = {}
    for m in first:
        committed[m.partition()] = max(committed.get(m.partition(), -1), m.offset() + 1)
    c.store_offsets(offsets=[TopicPartition(topic, p, o) for p, o in committed.items()])
    c.commit(asynchronous=False)
    c.close()

    # Second run, same group: the first offset per partition must be the committed one
    c2 = new_consumer(group)
    c2.subscribe([topic])
    seen_first = {}
    for m in drain(c2, 10):
        seen_first.setdefault(m.partition(), m.offset())
    c2.close()
    for p, off in seen_first.items():
        if p in committed:
            assert off == committed[p], f"partition {p}: resumed at {off}, committed {committed[p]}"


def test_topology_topics_exist_with_configured_partitions_and_retention():
    """The three topics `make topics` created match config/topology.toml."""
    if not broker_available():
        pytest.skip(f"no Kafka broker at {SETTINGS.kafka_bootstrap}")
    from confluent_kafka.admin import ConfigResource

    admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap})
    md = admin.list_topics(timeout=10).topics
    for t in SETTINGS.topology.topics.values():
        assert t.name in md, f"{t.name} missing: run make topics"
        assert len(md[t.name].partitions) == t.partitions
        res = ConfigResource(ConfigResource.Type.TOPIC, t.name)
        cfg = admin.describe_configs([res])[res].result(timeout=10)
        assert int(cfg["retention.ms"].value) == t.retention_ms
