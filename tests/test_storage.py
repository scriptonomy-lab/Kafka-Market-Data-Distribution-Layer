"""
Tests for Storage's write path and offset handling (pipeline/storage.py).

Unit tests (no Kafka, no database) use fakes with the same methods as
confluent-kafka's Message / Consumer and a fake Database, and pin down the
ORDER of operations the delivery guarantee depends on:

    database COMMIT  ->  store_offsets          (never the other way round)
    database down    ->  no store_offsets, seek back, retry
    on_lost          ->  never commits

Integration tests (marked `integration`) write to the real TimescaleDB from
docker compose and prove that redelivered messages are counted as
duplicates and never stored twice.
"""

import dataclasses
import json
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import KafkaException, TopicPartition

from pipeline.common import Settings, db_conninfo
from pipeline.storage import (
    Database,
    DatabaseUnavailable,
    first_offsets,
    kafka_time,
    next_offsets,
    persist_batch,
    rebalance_callbacks,
    row_from_message,
    store_offsets_safely,
)

SETTINGS = Settings.from_env()
FX = "marketdata.fx.quotes.v1"


class FakeMessage:
    """Just enough of confluent_kafka.Message for Storage's functions."""

    def __init__(self, partition, offset, seq, symbol="EURUSD", topic=FX, ts_ms=1_761_739_200_123):
        self._t, self._p, self._o, self._ts = topic, partition, offset, ts_ms
        self._v = json.dumps(
            {
                "schema_version": 2,
                "symbol": symbol,
                "asset_class": "fx",
                "bid": 1.1,
                "ask": 1.2,
                "mid": 1.15,
                "timestamp": "2026-10-29T12:00:00.000Z",
                "ingest_ts": "2026-10-29T12:00:00.010Z",
                "seq": seq,
                "session_id": "abc",
                "type": "quote",
                "source": "tradermade",
                "a_field_from_the_future": True,  # consumers must ignore unknown fields
            }
        ).encode()

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def timestamp(self):
        return (1, self._ts)


class Recorder:
    """Shared call log, so tests can assert the ORDER of DB and Kafka calls."""

    def __init__(self):
        self.calls = []


class FakeDatabase:
    def __init__(self, rec, fail=False, existing=0):
        self.rec, self.fail, self.existing = rec, fail, existing

    def write(self, rows, batch):
        if self.fail:
            self.rec.calls.append(("db_failed",))
            raise DatabaseUnavailable("connection refused")
        self.rec.calls.append(("db_commit", len(rows)))
        return len(rows) - self.existing


class FakeConsumer:
    def __init__(self, rec, not_assigned=()):
        self.rec, self.not_assigned = rec, set(not_assigned)

    def store_offsets(self, offsets):
        (tp,) = offsets
        if (tp.topic, tp.partition) in self.not_assigned:
            raise KafkaException("Local: Erroneous state")
        self.rec.calls.append(("store_offsets", tp.topic, tp.partition, tp.offset))

    def seek(self, tp):
        self.rec.calls.append(("seek", tp.topic, tp.partition, tp.offset))

    def commit(self, asynchronous=False):
        self.rec.calls.append(("commit",))

    def committed(self, partitions, timeout=None):
        return [TopicPartition(tp.topic, tp.partition, 42) for tp in partitions]

    def position(self, partitions):
        return [TopicPartition(tp.topic, tp.partition, 43) for tp in partitions]


class NullLog:
    """get_logger stand-in: log_event() calls logger.log(level, msg, extra=...)."""

    def __init__(self):
        self.events = []

    def log(self, level, msg, extra=None):
        self.events.append((msg, extra["fields"]))


# ------------------------------------------------------------------ pure helpers
def test_next_offsets_is_last_offset_plus_one_per_topic_partition():
    msgs = [FakeMessage(0, 4, 1), FakeMessage(0, 7, 2), FakeMessage(2, 1, 1, topic="other")]
    got = {(tp.topic, tp.partition, tp.offset) for tp in next_offsets(msgs)}
    assert got == {(FX, 0, 8), ("other", 2, 2)}


def test_first_offsets_is_where_a_failed_batch_is_reread_from():
    msgs = [FakeMessage(0, 7, 2), FakeMessage(0, 4, 1), FakeMessage(1, 9, 1)]
    got = {(tp.partition, tp.offset) for tp in first_offsets(msgs)}
    assert got == {(0, 4), (1, 9)}


def test_kafka_time_is_exact_to_the_millisecond():
    # Part of the primary key: a redelivery must map to the identical value
    t = kafka_time(FakeMessage(0, 0, 1, ts_ms=1_761_739_200_123))
    assert t == datetime(2025, 10, 29, 12, 0, 0, 123_000, tzinfo=UTC)


def test_row_carries_asset_class_and_instance_id_and_ignores_unknown_fields():
    m = FakeMessage(5, 10, 1)
    row = row_from_message(m, json.loads(m.value()), "storage-a1", datetime.now(UTC))
    assert row[:3] == (FX, 5, 10)
    assert row[5] == "fx" and row[15] == "storage-a1"


# --------------------------------------------- offsets only after the DB commit
def test_offsets_are_stored_only_after_the_database_commit():
    rec = Recorder()
    msgs = [FakeMessage(5, o, o + 1) for o in range(3)]
    result = persist_batch(msgs, FakeDatabase(rec), FakeConsumer(rec), SETTINGS, NullLog(), 0)
    assert rec.calls == [("db_commit", 3), ("store_offsets", FX, 5, 3)]
    assert (result["inserted"], result["duplicates"]) == (3, 0)


def test_database_down_stores_no_offset_and_rewinds_the_batch():
    rec = Recorder()
    msgs = [FakeMessage(5, 10, 1), FakeMessage(5, 11, 2), FakeMessage(1, 3, 1, "USDJPY")]
    with pytest.raises(DatabaseUnavailable):
        persist_batch(msgs, FakeDatabase(rec, fail=True), FakeConsumer(rec), SETTINGS, NullLog(), 0)
    assert not [c for c in rec.calls if c[0] == "store_offsets"]
    # Seek back to the FIRST offset of the batch on each partition
    assert set(rec.calls[1:]) == {("seek", FX, 5, 10), ("seek", FX, 1, 3)}


def test_retry_after_outage_counts_already_committed_rows_as_duplicates():
    # COMMIT reached the server but the reply was lost: the retry finds the rows
    rec = Recorder()
    msgs = [FakeMessage(5, o, o + 1) for o in range(4)]
    db = FakeDatabase(rec, existing=4)
    result = persist_batch(msgs, db, FakeConsumer(rec), SETTINGS, NullLog(), 0)
    assert (result["inserted"], result["duplicates"]) == (0, 4)
    assert rec.calls[-1] == ("store_offsets", FX, 5, 4)  # still marked done


def test_store_offsets_for_a_revoked_partition_is_skipped_not_fatal():
    rec, log = Recorder(), NullLog()
    consumer = FakeConsumer(rec, not_assigned={(FX, 1)})
    offsets = [TopicPartition(FX, 1, 5), TopicPartition(FX, 5, 9)]
    assert store_offsets_safely(consumer, offsets, log) == 1
    assert rec.calls == [("store_offsets", FX, 5, 9)]  # the partition we still own
    assert log.events[0][0] == "store_offsets_skipped"


# ------------------------------------------------------- rebalance callbacks
def test_on_lost_never_commits():
    rec, log = Recorder(), NullLog()
    first_seen = {(FX, 5)}
    _assign, _revoke, on_lost = rebalance_callbacks(log, first_seen)
    on_lost(FakeConsumer(rec), [TopicPartition(FX, 5)])
    assert rec.calls == []  # no commit, nothing stored
    assert log.events == [("lost", {"topic": FX, "partition": 5})]
    assert first_seen == set()  # a later re-assignment logs "resumed" again


def test_on_revoke_commits_before_logging_the_revocation():
    rec, log = Recorder(), NullLog()
    _assign, on_revoke, _lost = rebalance_callbacks(log, set())
    on_revoke(FakeConsumer(rec), [TopicPartition(FX, 2)])
    assert rec.calls == [("commit",)]
    name, fields = log.events[0]
    assert name == "revoked" and fields["committed_before_revoke"] is True
    assert (fields["topic"], fields["partition"], fields["offset"]) == (FX, 2, 43)


def test_on_assign_logs_the_committed_offset_per_topic_partition():
    rec, log = Recorder(), NullLog()
    on_assign, _revoke, _lost = rebalance_callbacks(log, set())
    on_assign(FakeConsumer(rec), [TopicPartition(FX, 2), TopicPartition("c", 0)])
    assert [(e, f["topic"], f["partition"], f["offset"]) for e, f in log.events] == [
        ("assigned", FX, 2, 42),
        ("assigned", "c", 0, 42),
    ]
    assert rec.calls == []  # assign() is NOT called: the client does it


# ------------------------------------------------- TimescaleDB (integration)
def database_available() -> bool:
    try:
        import psycopg

        psycopg.connect(db_conninfo(SETTINGS)).close()
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_redelivered_messages_are_ignored_and_counted_on_timescaledb():
    if not database_available():
        pytest.skip("TimescaleDB not reachable (make up)")
    import psycopg

    topic = f"it-storage-{uuid.uuid4().hex[:8]}"  # isolates this test's rows
    settings = dataclasses.replace(SETTINGS, instance_id="it-test")
    db = Database(db_conninfo(settings))
    rec = Recorder()
    consumer = FakeConsumer(rec)

    def batch(offsets):
        return [FakeMessage(0, o, o + 1, topic=topic, ts_ms=1_761_739_200_000 + o) for o in offsets]

    try:
        first = persist_batch(batch(range(5)), db, consumer, settings, NullLog(), 0)
        # After a crash, Kafka redelivers offsets 3-4 plus new 5-6
        again = persist_batch(batch(range(3, 7)), db, consumer, settings, NullLog(), 0)
        assert (first["inserted"], first["duplicates"]) == (5, 0)
        assert (again["inserted"], again["duplicates"]) == (2, 2)
        with psycopg.connect(db_conninfo(settings)) as conn:
            n = conn.execute("SELECT count(*) FROM ticks WHERE topic = %s", (topic,)).fetchone()[0]
            dups = conn.execute(
                "SELECT sum(duplicates) FROM storage_batches WHERE %s = ANY(partitions)",
                (f"{topic}:0",),
            ).fetchone()[0]
        assert n == 7 and dups == 2
    finally:
        db.close()
        with psycopg.connect(db_conninfo(settings), autocommit=True) as conn:
            conn.execute("DELETE FROM ticks WHERE topic = %s", (topic,))
            conn.execute("DELETE FROM storage_batches WHERE %s = ANY(partitions)", (f"{topic}:0",))


@pytest.mark.integration
def test_unreachable_database_raises_database_unavailable():
    settings = dataclasses.replace(SETTINGS, db_host="127.0.0.1", db_port=1)  # nothing listens
    db = Database(db_conninfo(settings))
    with pytest.raises(DatabaseUnavailable):
        db.write([("x",) * 17], {})
