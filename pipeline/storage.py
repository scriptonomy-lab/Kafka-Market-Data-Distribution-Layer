"""
Storage consumer — consumer group "storage-svc", N instances.

Workload: persist every event of all three topics to TimescaleDB so nothing
is lost, even across crashes, and scale out by adding instances. This is the
consumer the experiments stop, kill, slow down and scale, so its offset
handling is written to be read and quoted.

The delivery guarantee, step by step
------------------------------------
Kafka remembers a group's position as a COMMITTED OFFSET ("next message to
read"). The order of two operations decides what a crash costs:

    commit offset, then write row   -> crash in between = row LOST    (at-most-once)
    write row, then commit offset   -> crash in between = row RE-READ (at-least-once)

This service always does the second, and makes re-reads harmless: every row's
primary key is the message's address in Kafka, (topic, partition,
kafka_offset, kafka_ts), and rows are written with INSERT ... ON CONFLICT DO
NOTHING RETURNING. Rows returned = rows inserted; the rest of the batch were
redeliveries, counted as "duplicates suppressed". At-least-once delivery + an
idempotent write gives an effectively-once result. (Why kafka_ts is part of
the key: db/migrations/001_ticks.sql.)

One batch, in order
-------------------
    1. consume() up to STORAGE_BATCH_SIZE messages
    2. ONE database transaction: insert the ticks + one storage_batches row
    3. COMMIT                              <- rows are durable ...
    4. store_offsets(last offset + 1)      <- ... only then marked done locally
    5. every STORAGE_OFFSET_COMMIT_INTERVAL_S: commit the stored offsets to
       Kafka ASYNCHRONOUSLY; on revoke and shutdown: SYNCHRONOUSLY

Why two kinds of commit: a synchronous commit blocks the consume loop until
the broker answers, and during a rebalance that answer can take seconds (the
coordinator replies only when the group settles, often with
ILLEGAL_GENERATION). In a rehearsal of experiment F the periodic sync commit
of one instance collided with a scale-out and stalled ALL its partitions,
including those the cooperative rebalance never moved, for ~2.2 s. An async
commit never blocks; if it fails, the same stored offsets go out with the
next commit, so at-least-once is unaffected. Where the commit must have
landed before we continue (handing partitions over, leaving the group), it
stays synchronous.

`enable.auto.offset.store=False` is what makes step 4 explicit: the client
never marks a message done just because consume() returned it.

If the database is unreachable
------------------------------
Step 2 fails, so step 4 never happens for that batch: no offset is stored for
a row that was not written. The consumer SEEKS back to the first offset of the
batch on each of its partitions and waits (1, 2, 4 ... 30 s); the next
consume() re-reads the same messages and tries again. The loop keeps calling
consume(), so the instance stays in its group while it waits. If a COMMIT was
sent but its answer was lost, the retry finds the rows already present and
counts them as duplicates: idempotency covers that ambiguity too.

Scale-out: several instances, one group
---------------------------------------
Run N copies (`docker compose up -d --scale storage=N`). Kafka splits the
partitions of all three topics between them. The group uses the
cooperative-sticky assignor (pipeline/common.py:consumer_config): on a join
or leave, only the partitions that must move are revoked; the others keep
being consumed. The rebalance callbacks:

    on_assign  log each newly assigned topic-partition with its committed
               offset (where this instance will resume)
    on_revoke  commit the stored offsets FIRST, then log: the next owner
               starts exactly after the last row we wrote (0 duplicates)
    on_lost    log only. The partitions were taken away (e.g. this instance
               missed its session timeout); another member may already own
               them, so committing now could overwrite its progress.

None of them calls assign(): with the cooperative protocol the client applies
the incremental assignment itself after the callback returns.

instance_id = the container hostname (unique per replica). It is on every
log line, every row and every storage_batches record, and names the log
file: logs/storage-<instance_id>.log.

Fault knobs (used by the experiments)
-------------------------------------
* STORAGE_DELAY_MS   sleep this long per message before writing (slow sink)
* SIGTERM            clean stop (`docker stop`): finish batch, commit, leave group
* SIGKILL            crash (`docker kill -s KILL`): nothing runs; Kafka
                     redelivers from the last commit after session.timeout.ms

Run:  docker compose up -d --scale storage=2   (or on the host: make storage)
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import UTC, datetime, timedelta

import psycopg
from confluent_kafka import OFFSET_INVALID, Consumer, KafkaError, KafkaException, TopicPartition

from pipeline.common import (
    GROUP_STORAGE,
    OrderingChecker,
    Settings,
    StopFlag,
    consumer_config,
    db_conninfo,
    get_logger,
    log_event,
    now_ms,
    utc_now,
)

STATS_EVERY_S = 10.0
# consume() returns when the batch is full OR this much time has passed. At a
# few dozen ticks/s batches never fill, so this is effectively the batching
# delay Storage adds: kept short so stored data trails the market by well
# under a second while still writing several messages per transaction.
POLL_TIMEOUT_S = 0.25
DB_RETRY_START_S = 1.0
DB_RETRY_MAX_S = 30.0  # far below max.poll.interval.ms (300 s): we stay in the group

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Column order of a `ticks` row; row_from_message() builds tuples in this order.
TICK_COLUMNS = (
    "topic",
    "partition",
    "kafka_offset",
    "kafka_ts",
    "symbol",
    "asset_class",
    "bid",
    "ask",
    "mid",
    "provider_ts",
    "ingest_ts",
    "seq",
    "session_id",
    "event_type",
    "source",
    "instance_id",
    "stored_at",
)
TICK_TYPES = (
    "text",
    "integer",
    "bigint",
    "timestamptz",
    "text",
    "text",
    "float8",
    "float8",
    "float8",
    "timestamptz",
    "timestamptz",
    "bigint",
    "text",
    "text",
    "text",
    "text",
    "timestamptz",
)

# ONE statement per batch: each column travels as one array and unnest()
# turns the arrays back into rows. ON CONFLICT DO NOTHING skips rows whose
# primary key exists (redeliveries); RETURNING yields one row per row
# actually inserted, so len(result) = inserted.
INSERT_TICKS_SQL = (
    f"INSERT INTO ticks ({', '.join(TICK_COLUMNS)}) "
    f"SELECT * FROM unnest({', '.join(f'%s::{t}[]' for t in TICK_TYPES)}) "
    "ON CONFLICT DO NOTHING RETURNING 1"
)

INSERT_BATCH_SQL = """
INSERT INTO storage_batches (written_at, instance_id, messages, inserted, duplicates,
    max_age_ms, min_age_ms, delay_ms, partitions)
VALUES (%(written_at)s, %(instance_id)s, %(messages)s, %(inserted)s, %(duplicates)s,
    %(max_age_ms)s, %(min_age_ms)s, %(delay_ms)s, %(partitions)s)
"""


class DatabaseUnavailable(Exception):
    """The database could not be reached; the batch was NOT written."""


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def kafka_time(msg) -> datetime:
    """Kafka CreateTime (epoch ms) as an exact datetime. Integer milliseconds
    via timedelta, not float seconds, so a redelivered message always maps to
    the identical value (it is part of the primary key)."""
    return EPOCH + timedelta(milliseconds=msg.timestamp()[1])


def row_from_message(msg, e: dict, instance_id: str, stored_at: datetime) -> tuple:
    """One Kafka message + its decoded event -> one `ticks` row (TICK_COLUMNS order)."""
    return (
        msg.topic(),
        msg.partition(),
        msg.offset(),
        kafka_time(msg),
        e["symbol"],
        # Tolerate schema_version 1 events (no asset_class) and unknown fields
        e.get("asset_class", "unknown"),
        e["bid"],
        e["ask"],
        e["mid"],
        parse_iso(e["timestamp"]),
        parse_iso(e["ingest_ts"]),
        e["seq"],
        e["session_id"],
        e["type"],
        e.get("source", "unknown"),
        instance_id,
        stored_at,
    )


class Database:
    """One connection to TimescaleDB, re-opened after any connection failure.

    write() is the whole persistence step: one transaction, committed before
    it returns. A connection problem surfaces as DatabaseUnavailable so the
    caller can tell "not written, retry" from a real bug (which raises as is).
    """

    def __init__(self, conninfo: str) -> None:
        self.conninfo = conninfo
        self.conn: psycopg.Connection | None = None

    def write(self, rows: list[tuple], batch: dict) -> int:
        """Insert `rows` and the storage_batches record in ONE transaction;
        return how many rows were new. Raises DatabaseUnavailable."""
        try:
            if self.conn is None or self.conn.closed:
                # autocommit=True: nothing is open between batches; the
                # transaction() block below is the only transaction.
                self.conn = psycopg.connect(self.conninfo, autocommit=True)
            with self.conn.transaction():  # BEGIN ... COMMIT on exit
                columns = [list(col) for col in zip(*rows, strict=True)]
                inserted = len(self.conn.execute(INSERT_TICKS_SQL, columns).fetchall())
                record = {**batch, "inserted": inserted, "duplicates": len(rows) - inserted}
                self.conn.execute(INSERT_BATCH_SQL, record)
            return inserted
        except psycopg.OperationalError as exc:
            self.close()
            raise DatabaseUnavailable(f"{type(exc).__name__}: {exc}") from None

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except psycopg.Error:
                pass
            self.conn = None


def next_offsets(messages) -> list[TopicPartition]:
    """For a batch, the offset to resume from per topic-partition = last
    offset + 1 (Kafka's convention: a committed offset names the NEXT message)."""
    nxt: dict[tuple[str, int], int] = {}
    for m in messages:
        key = (m.topic(), m.partition())
        nxt[key] = max(nxt.get(key, -1), m.offset() + 1)
    return [TopicPartition(t, p, o) for (t, p), o in sorted(nxt.items())]


def first_offsets(messages) -> list[TopicPartition]:
    """The first offset of a batch per topic-partition (where to seek back to)."""
    first: dict[tuple[str, int], int] = {}
    for m in messages:
        key = (m.topic(), m.partition())
        first[key] = min(first.get(key, m.offset()), m.offset())
    return [TopicPartition(t, p, o) for (t, p), o in sorted(first.items())]


def store_offsets_safely(consumer, offsets: list[TopicPartition], log) -> int:
    """store_offsets() one topic-partition at a time; return how many stored.

    A partition can stop being ours between consume() and this call (a
    rebalance served while we were retrying the database). Storing for it
    then fails; that must not crash the instance. The row IS in the database,
    and the new owner re-reads from the last committed offset, so the worst
    case is a redelivery that the primary key absorbs. Storing per partition
    keeps the partitions we still own unaffected."""
    stored = 0
    for tp in offsets:
        try:
            consumer.store_offsets(offsets=[tp])
            stored += 1
        except KafkaException as exc:
            log_event(
                log,
                "store_offsets_skipped",
                logging.WARNING,
                topic=tp.topic,
                partition=tp.partition,
                offset=tp.offset,
                reason=str(exc),
            )
    return stored


def rewind(consumer, messages, log) -> None:
    """Seek back to the first offset of the unwritten batch on every
    partition, so the next consume() delivers the same messages again."""
    for tp in first_offsets(messages):
        try:
            consumer.seek(tp)
        except KafkaException as exc:
            # No longer assigned: its new owner resumes from the committed offset.
            log_event(
                log, "rewind_skipped", topic=tp.topic, partition=tp.partition, reason=str(exc)
            )


def persist_batch(good, db: Database, consumer, settings: Settings, log, delay_ms: int) -> dict:
    """Steps 2-4 for one batch. Returns {"events", "inserted", "duplicates"}.

    The ONLY place offsets are stored, and only after Database.write()
    returned, i.e. after the COMMIT succeeded. If the database is unreachable
    it rewinds the consumer and re-raises DatabaseUnavailable; no offset is
    stored for any message of the batch."""
    # Ages measured right before the write: how stale is the data this
    # service is acting on? Large during catch-up, ~0 when live.
    t_now = now_ms()
    ages = [t_now - m.timestamp()[1] for m in good]
    events = [json.loads(m.value()) for m in good]
    stored_at = utc_now()
    rows = [
        row_from_message(m, e, settings.instance_id, stored_at)
        for m, e in zip(good, events, strict=True)
    ]
    batch = {
        "written_at": stored_at,
        "instance_id": settings.instance_id,
        "messages": len(good),
        "max_age_ms": max(ages),
        "min_age_ms": min(ages),
        "delay_ms": delay_ms,
        "partitions": sorted({f"{m.topic()}:{m.partition()}" for m in good}),
    }
    try:
        inserted = db.write(rows, batch)  # 2) + 3) one transaction, committed
    except DatabaseUnavailable:
        rewind(consumer, good, log)  # nothing stored: re-read this batch next time
        raise
    store_offsets_safely(consumer, next_offsets(good), log)  # 4) only now: mark done
    return {"events": events, "inserted": inserted, "duplicates": len(good) - inserted}


def rebalance_callbacks(log, first_seen: set):
    """The three rebalance callbacks, built here (not inside main) so tests
    can call them with a fake consumer. `first_seen` is the set of
    topic-partitions whose first offset has been logged as "resumed"; a
    partition that moves away and comes back must log "resumed" again."""

    def on_assign(c, partitions):
        # Only the NEWLY assigned partitions (cooperative protocol). Ask the
        # broker where this group will resume each one and log it: after a
        # restart or a move, these are the offsets the experiments check.
        committed = c.committed(partitions, timeout=10)
        for tp in committed:
            first_seen.discard((tp.topic, tp.partition))
            # OFFSET_INVALID (-1001) = never committed (empty partition, or a
            # brand-new group): it starts from offset_reset ("earliest").
            log_event(
                log,
                "assigned",
                topic=tp.topic,
                partition=tp.partition,
                offset="none" if tp.offset == OFFSET_INVALID else tp.offset,
            )

    def on_revoke(c, partitions):
        # These partitions move to another member. Every message consumed so
        # far is already in the database with its offset stored (callbacks
        # run inside consume(), never mid-batch), so commit now: the next
        # owner starts right after our last row, with no redelivery.
        try:
            c.commit(asynchronous=False)
            committed = True
        except KafkaException as exc:
            committed = False  # e.g. _NO_OFFSET: nothing new since last commit
            log_event(log, "revoke_commit_skipped", reason=str(exc))
        for tp in c.position(partitions):
            first_seen.discard((tp.topic, tp.partition))
            log_event(
                log,
                "revoked",
                topic=tp.topic,
                partition=tp.partition,
                offset=tp.offset if tp.offset >= 0 else "none",
                committed_before_revoke=committed,
            )

    def on_lost(c, partitions):
        # Lost, not revoked: the group already moved on without us (e.g. we
        # missed the session timeout). Do NOT commit: another member may own
        # these partitions now and our commit could rewind or skip its work.
        for tp in partitions:
            first_seen.discard((tp.topic, tp.partition))
            log_event(log, "lost", logging.WARNING, topic=tp.topic, partition=tp.partition)

    return on_assign, on_revoke, on_lost


def main() -> int:
    settings = Settings.from_env()
    # One log file per instance: logs/storage-<instance_id>.log
    log = get_logger("storage", settings, per_instance=True)
    stop = StopFlag()
    db = Database(db_conninfo(settings))
    ordering = OrderingChecker()

    def on_commit(err, partitions):
        # Result of an asynchronous commit, delivered inside consume().
        # _NO_OFFSET only means "nothing new was stored": not a failure.
        if err is not None and err.code() != KafkaError._NO_OFFSET:
            log_event(log, "commit_failed", logging.WARNING, reason=str(err))

    consumer = Consumer(
        consumer_config(
            settings,
            GROUP_STORAGE,
            auto_commit=False,
            offset_reset="earliest",  # a new storage group must not skip history
            **{
                "enable.auto.offset.store": False,  # we store offsets after the DB commit
                "on_commit": on_commit,
            },
        )
    )

    # (topic, partition) whose first offset since assignment we already logged
    first_seen: set[tuple[str, int]] = set()
    on_assign, on_revoke, on_lost = rebalance_callbacks(log, first_seen)
    consumer.subscribe(
        settings.topology.topic_names,
        on_assign=on_assign,
        on_revoke=on_revoke,
        on_lost=on_lost,
    )

    delay_ms = settings.storage_delay_ms
    # In slow mode, read about one second of work per batch so the lag curve
    # moves smoothly instead of in huge steps (and SIGTERM is honoured fast).
    batch_size = settings.storage_batch_size
    if delay_ms:
        batch_size = max(1, min(batch_size, 1000 // delay_ms))
    pending_commit = False
    db_retry_s = 0.0  # 0 = database healthy
    totals = {"messages": 0, "inserted": 0, "duplicates": 0}
    last_commit = last_stats = time.monotonic()
    log_event(
        log,
        "started",
        group=GROUP_STORAGE,
        topics=settings.topology.topic_names,
        db=f"{settings.db_host}:{settings.db_port}/{settings.db_name}",
        batch_size=batch_size,
        delay_ms=delay_ms,
        commit_interval_s=settings.storage_offset_commit_interval_s,
    )

    while not stop.is_set():
        # 1) Up to N messages, waiting at most POLL_TIMEOUT_S. Rebalance
        # callbacks above run inside this call.
        batch = consumer.consume(num_messages=batch_size, timeout=POLL_TIMEOUT_S)
        good = []
        for m in batch:
            if m.error():
                log_event(log, "consumer_error", logging.WARNING, error=str(m.error()))
            else:
                good.append(m)

        if good:
            for m in good:
                key = (m.topic(), m.partition())
                if key not in first_seen:
                    # The first offset read after (re)assignment, per
                    # topic-partition. Must equal the committed offset
                    # logged by on_assign.
                    first_seen.add(key)
                    log_event(
                        log,
                        "resumed",
                        topic=m.topic(),
                        partition=m.partition(),
                        offset=m.offset(),
                    )

            if delay_ms:
                # Simulated slow sink: caps throughput at 1000/delay_ms msg/s.
                # stop.wait() instead of time.sleep() so SIGTERM cuts it short.
                stop.wait(delay_ms / 1000.0 * len(good))

            try:
                result = persist_batch(good, db, consumer, settings, log, delay_ms)
            except DatabaseUnavailable as exc:
                db_retry_s = min(max(DB_RETRY_START_S, db_retry_s * 2), DB_RETRY_MAX_S)
                log_event(
                    log,
                    "db_unavailable",
                    logging.WARNING,
                    reason=str(exc),
                    batch_messages=len(good),
                    retry_in_s=db_retry_s,
                )
                stop.wait(db_retry_s)
                continue
            if db_retry_s:
                log_event(log, "db_recovered")
                db_retry_s = 0.0
            pending_commit = True
            for e in result["events"]:  # ordering check on what we just stored
                ordering.observe(e["session_id"], e["symbol"], e["seq"])
            totals["messages"] += len(good)
            totals["inserted"] += result["inserted"]
            totals["duplicates"] += result["duplicates"]

        # 5) Periodically send the stored offsets to Kafka, WITHOUT blocking
        # the loop (see "Why two kinds of commit" above). The outcome arrives
        # in on_commit; a failed commit is retried by the next one.
        now = time.monotonic()
        if pending_commit and now - last_commit >= settings.storage_offset_commit_interval_s:
            try:
                consumer.commit(asynchronous=True)
                pending_commit = False
            except KafkaException as exc:
                log_event(log, "commit_failed", logging.WARNING, reason=str(exc))
            last_commit = now

        if now - last_stats >= STATS_EVERY_S:
            log_event(
                log,
                "stats",
                **totals,
                assigned=len(consumer.assignment()),
                **ordering.as_dict(),
            )
            last_stats = now

    # Clean shutdown (SIGTERM / Ctrl-C): final commit, then leave the
    # group so the broker reassigns partitions immediately.
    if pending_commit:
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            log_event(log, "final_commit_failed", logging.WARNING, reason=str(exc))
    consumer.close()  # runs on_revoke for what we still own, then LeaveGroup
    db.close()
    log_event(log, "stopped", signal=stop.signal_name, **totals, **ordering.as_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
