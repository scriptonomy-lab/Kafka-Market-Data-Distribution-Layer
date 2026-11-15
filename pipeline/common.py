"""
Shared plumbing used by every service in the pipeline.

Why this module exists
----------------------
Each service (producer, pricing, risk, storage, monitor) is deliberately a
standalone script with its own main loop, so a reader can open ONE file and
understand ONE workload. The handful of things every service needs live here
so they are written once and behave identically everywhere:

    Settings            typed configuration from environment variables / .env,
                        plus the topology (config/topology.toml)
    get_logger          JSON-lines file log (machine-readable evidence) plus an
                        optional human-readable console stream
    log_event           write one structured event line
    producer_config     the Kafka producer settings, in one place
    consumer_config     the Kafka consumer settings, in one place
    StopFlag            turns Ctrl-C / SIGTERM into a clean "please stop" flag
    compose_service_running / producer_running
                        is a Docker Compose service up? (one session per key)
    OrderingChecker     per-symbol sequence checks (gaps, duplicates)
    sample_groups       consumer-group lag + member assignments over all topics
                        (monitor, preflight, experiments)
    derive_status       RUNNING / STALLED / DOWN from a lag sample

Nothing in this module talks to TraderMade.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

from pipeline.topology import DEFAULT_TOPOLOGY_FILE, Topology, load_topology

# -----------------------------------------------------------------------------
# Consumer-group names. One group per workload: Kafka delivers EVERY message
# to EVERY group, and each group keeps its own committed offsets. Changing a
# name creates a brand-new group with no history, so treat these as fixed.
# -----------------------------------------------------------------------------
GROUP_PRICING = "pricing-svc"
GROUP_RISK = "risk-svc"
GROUP_STORAGE = "storage-svc"
GROUP_MONITOR = "monitor-svc"
ALL_GROUPS = (GROUP_PRICING, GROUP_RISK, GROUP_STORAGE, GROUP_MONITOR)

# The repository root (this file is <root>/pipeline/common.py). Relative paths
# in .env (data/, logs/, ...) are resolved against it, so the services behave
# the same no matter which directory you launch them from.
REPO_ROOT = Path(__file__).resolve().parent.parent


# =============================================================================
# Time helpers
# =============================================================================
def utc_now() -> datetime:
    """Current time as a timezone-aware UTC datetime (never a naive one)."""
    return datetime.now(UTC)


def iso_ms(dt: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with milliseconds, e.g.
    '2026-10-29T13:05:12.713Z'. One format everywhere keeps logs, events and
    database timestamps directly comparable."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_ms() -> int:
    """Wall-clock time in epoch milliseconds — the unit Kafka uses for message
    timestamps, so `now_ms() - msg.timestamp()[1]` is a message's age."""
    return time.time_ns() // 1_000_000


# =============================================================================
# Settings
# =============================================================================
def _env(name: str, default: str = "") -> str:
    """Read one variable, treating unset and empty the same way."""
    value = os.environ.get(name, "").strip()
    return value if value else default


def _path(name: str, default: str) -> Path:
    """Read a path variable; relative paths are anchored at the repo root."""
    p = Path(_env(name, default))
    return p if p.is_absolute() else REPO_ROOT / p


# Created by `make setup` (random, git- and docker-ignored). Mounted into the
# database and Storage containers as the Compose secret `db_password`.
DEFAULT_DB_PASSWORD_FILE = REPO_ROOT / "secrets" / "db_password"


def _read_quietly(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def _secret(name: str) -> str:
    """A secret from the variable NAME, or else from the file named by
    NAME_FILE. Containers get secrets as files under /run/secrets/ (Docker
    Compose `secrets:`), which keeps them out of `docker inspect` output;
    host tools get them from .env as before."""
    value = _env(name)
    if value:
        return value
    file_name = _env(f"{name}_FILE")
    if file_name:
        return _read_quietly(Path(file_name))
    return ""


@dataclass(frozen=True)
class Settings:
    """Every tunable value in one immutable object.

    Built once at start-up by `Settings.from_env()`. Values come from real
    environment variables first, then from `.env` (real variables win — the
    experiment runner relies on that to start Storage with a different
    STORAGE_DELAY_MS without editing any file).
    """

    # TraderMade
    ws_url: str
    ws_key: str
    rest_url: str
    rest_key: str
    # Topics, symbols and instrument metadata (config/topology.toml)
    topology: Topology
    # Kafka
    kafka_bootstrap: str
    # Producer
    stale_after_s: float
    ws_ping_interval_s: float | None
    # TimescaleDB (Storage, db_init, experiments). The password is a secret:
    # POSTGRES_PASSWORD, or the file POSTGRES_PASSWORD_FILE names (containers:
    # /run/secrets/db_password), or secrets/db_password made by `make setup`.
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    # Storage
    storage_batch_size: int
    storage_delay_ms: int
    storage_offset_commit_interval_s: float
    # Which process this is. In Docker the container hostname (unique per
    # replica, e.g. one per Storage instance); on a laptop the host name.
    instance_id: str
    # Local folders
    log_dir: Path
    run_dir: Path
    results_dir: Path

    @classmethod
    def from_env(cls) -> Settings:
        # override=False: an already-set environment variable beats .env
        load_dotenv(REPO_ROOT / ".env", override=False)
        return cls(
            ws_url=_env("TRADERMADE_WS_URL", "wss://stream.tradermade.com/feedAdv"),
            ws_key=_secret("TRADERMADE_WS_KEY"),
            rest_url=_env("TRADERMADE_REST_URL", "https://marketdata.tradermade.com/api/v1"),
            rest_key=_secret("TRADERMADE_REST_KEY"),
            topology=load_topology(_path("TOPOLOGY_FILE", str(DEFAULT_TOPOLOGY_FILE))),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "localhost:9092"),
            stale_after_s=float(_env("STALE_AFTER_S", "60")),
            # 0 disables protocol-level pings (see .env.example)
            ws_ping_interval_s=float(_env("WS_PING_INTERVAL_S", "20")) or None,
            db_host=_env("POSTGRES_HOST", "localhost"),
            # Containers set POSTGRES_PORT=5432 (the internal port). Host tools
            # use the published port, POSTGRES_HOST_PORT, which docker-compose.yml
            # also reads, so one setting moves both if 5432 is taken locally.
            db_port=int(_env("POSTGRES_PORT", _env("POSTGRES_HOST_PORT", "5432"))),
            db_name=_env("POSTGRES_DB", "tradermade"),
            db_user=_env("POSTGRES_USER", "tradermade"),
            db_password=_secret("POSTGRES_PASSWORD") or _read_quietly(DEFAULT_DB_PASSWORD_FILE),
            storage_batch_size=int(_env("STORAGE_BATCH_SIZE", "500")),
            storage_delay_ms=int(_env("STORAGE_DELAY_MS", "0")),
            storage_offset_commit_interval_s=float(_env("STORAGE_OFFSET_COMMIT_INTERVAL_S", "5")),
            instance_id=_env("INSTANCE_ID", socket.gethostname()),
            log_dir=_path("LOG_DIR", "logs"),
            run_dir=_path("RUN_DIR", "run"),
            results_dir=_path("RESULTS_DIR", "results"),
        )


def db_conninfo(settings: Settings) -> str:
    """libpq connection string for psycopg. Never log it: it holds the password."""
    from psycopg.conninfo import make_conninfo

    return make_conninfo(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=5,
        application_name=f"tradermade-{settings.instance_id}",
    )


# =============================================================================
# Logging
#
# Two streams per service:
#   * logs/<service>.log  — one JSON object per line. Machine-readable, so the
#     experiment runner can parse it (e.g. Storage's "resumed" events) and you
#     can grep it for evidence. These lines are the "real logs" the brief asks
#     for.
#   * console             — short human-readable lines. Disabled for services
#     that draw a live `rich` table, because log lines would tear the table.
# =============================================================================
class _JsonLinesFormatter(logging.Formatter):
    """Render each record as a single JSON line with a stable set of keys.
    Every line names the instance (container hostname) that wrote it, so
    lines from several Storage replicas can be merged and still told apart."""

    def __init__(self, instance_id: str) -> None:
        super().__init__()
        self.instance_id = instance_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": iso_ms(datetime.fromtimestamp(record.created, UTC)),
            "level": record.levelname,
            "service": record.name,
            "instance_id": self.instance_id,
            "pid": record.process,
            "event": getattr(record, "event", "log"),
        }
        # Structured fields passed through log_event(..., **fields)
        payload.update(getattr(record, "fields", {}))
        if getattr(record, "event", None) is None:
            payload["msg"] = record.getMessage()
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # default=str keeps logging from ever crashing on an odd value type
        return json.dumps(payload, default=str)


class _ConsoleFormatter(logging.Formatter):
    """Render records as 'HH:MM:SS service LEVEL event key=value ...'."""

    def format(self, record: logging.LogRecord) -> str:
        clock = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        fields = getattr(record, "fields", {})
        tail = " ".join(f"{k}={v}" for k, v in fields.items())
        head = getattr(record, "event", None) or record.getMessage()
        return f"{clock} {record.name:<9} {record.levelname:<7} {head} {tail}".rstrip()


def get_logger(
    service: str, settings: Settings, console: bool = True, per_instance: bool = False
) -> logging.Logger:
    """Create the logger for one service.

    Args:
        service: short name; becomes the log file name and the "service" key.
        settings: used for the log directory and the instance id.
        console:  False for services that render a live table on the terminal.
        per_instance: True for scaled services (Storage): the file becomes
                  logs/<service>-<instance_id>.log, one per replica, so two
                  containers never interleave writes into one file.
    """
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(service)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # don't double-print through the root logger
    logger.handlers.clear()  # safe if get_logger is called twice (tests)

    stem = f"{service}-{settings.instance_id}" if per_instance else service
    file_handler = logging.FileHandler(settings.log_dir / f"{stem}.log", encoding="utf-8")
    file_handler.setFormatter(_JsonLinesFormatter(settings.instance_id))
    logger.addHandler(file_handler)

    if console:
        stream = logging.StreamHandler(sys.stdout)
        stream.setFormatter(_ConsoleFormatter())
        logger.addHandler(stream)
    return logger


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    """Write one structured event, e.g.
    log_event(log, "resumed", partition=0, offset=1042)
    -> {"ts": ..., "service": "storage", "event": "resumed", "partition": 0, "offset": 1042}
    """
    logger.log(level, event, extra={"event": event, "fields": fields})


# =============================================================================
# Kafka client configuration
#
# Kept side by side so the article can show both and so no service invents
# its own settings. Every key is a librdkafka property:
# https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
# =============================================================================
def producer_config(settings: Settings) -> dict:
    return {
        "bootstrap.servers": settings.kafka_bootstrap,
        "client.id": "tradermade-producer",
        # Idempotence gives every produce request a sequence number per
        # partition, so the broker drops duplicates caused by retries and
        # never reorders a key's messages. librdkafka leaves it OFF by
        # default (the Java client turns it on), so set it explicitly.
        "enable.idempotence": True,
        # Wait for the write to be fully acknowledged (required by idempotence).
        "acks": "all",
        # Hold messages up to 5 ms to batch them; tiny delay, fewer requests.
        "linger.ms": 5,
        # Cheap, fast compression for small JSON messages.
        "compression.type": "lz4",
        # Hash keys exactly like the Java client (murmur2). librdkafka's
        # default (CRC32-based) would send the same key to a DIFFERENT
        # partition than a JVM producer would, silently breaking per-symbol
        # ordering if both ever wrote to this topic.
        "partitioner": "murmur2_random",
    }


def consumer_config(
    settings: Settings,
    group_id: str,
    *,
    auto_commit: bool,
    offset_reset: str,
    **overrides,
) -> dict:
    """Kafka consumer settings shared by all four consumer services.

    Args:
        group_id:     the consumer group (one per workload, see ALL_GROUPS).
        auto_commit:  True  -> the client commits read positions every second
                               (fine for "latest state" services);
                      False -> the service decides when to commit (Storage
                               commits only after its database write).
        offset_reset: where a group with NO committed offset starts:
                      "earliest" (beginning of retained history) or
                      "latest" (only new messages). Ignored once the group
                      has committed offsets — restarts always resume there.
        overrides:    any extra librdkafka property, e.g. enable_auto_offset_store
                      is passed as {"enable.auto.offset.store": False}.
    """
    cfg = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": group_id,
        # Names the member in `kafka-consumer-groups --describe --members` and
        # in DescribeGroups, which is how monitoring maps a member to its
        # container. Not the PID: inside a container every service is PID 1.
        "client.id": f"{group_id}-{settings.instance_id}",
        # Cooperative (incremental) rebalancing: when a member joins or leaves,
        # only the partitions that must MOVE are revoked; every other member
        # keeps consuming its partitions throughout. The eager default
        # ("range,roundrobin") stops the whole group on every change.
        # Experiment F measures whether unmoved partitions really kept going.
        "partition.assignment.strategy": "cooperative-sticky",
        "enable.auto.commit": auto_commit,
        "auto.offset.reset": offset_reset,
        # If a consumer dies without saying goodbye (SIGKILL), the broker
        # waits this long before giving its partitions to someone else.
        # Default is 45 s; 10 s keeps the crash experiment readable.
        "session.timeout.ms": 10_000,
        # Heartbeats must be well inside the session timeout (rule of thumb: 1/3).
        "heartbeat.interval.ms": 3_000,
    }
    if auto_commit:
        # Default is 5 s, which makes lag curves jagged and misleading.
        cfg["auto.commit.interval.ms"] = 1_000
    cfg.update(overrides)
    return cfg


# =============================================================================
# Process control: clean shutdown
# =============================================================================
class StopFlag:
    """Turns SIGINT (Ctrl-C), SIGTERM and SIGHUP into a flag the main loop checks.

    In Docker, `docker stop` sends SIGTERM (via tini, see docker-compose.yml
    `init: true`) and waits stop_grace_period before SIGKILL. SIGHUP matters
    for services run in a terminal that gets closed: Python's default for
    SIGHUP is to die on the spot, with no final offset commit and no
    consumer.close(), so the group keeps a dead member until the session
    timeout.

    Main loops look like:
        stop = StopFlag()
        while not stop.is_set():
            ...
    so a signal never interrupts a half-written database batch: the loop
    finishes the current iteration, then exits through its cleanup code
    (final commit, consumer.close()). SIGKILL cannot be caught — that is
    exactly why experiment B uses it to simulate a crash.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self.signal_name: str | None = None
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._handle)

    def _handle(self, signum, _frame) -> None:
        self.signal_name = signal.Signals(signum).name
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        """Sleep up to `timeout` seconds, waking early if a stop arrives."""
        return self._event.wait(timeout)


def compose_service_running(service: str) -> bool:
    """True if the Docker Compose service has a running container. False if
    not, or if Docker is unavailable (then nothing of ours runs in it)."""
    try:
        out = subprocess.run(
            ["docker", "compose", "ps", "--status", "running", "--quiet", service],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return out.returncode == 0 and bool(out.stdout.strip())


def producer_running() -> bool:
    """Is the producer container holding the TraderMade session right now?
    Checked before anything else logs in with the same key (one session per
    key). v2 runs every service in Docker, so Docker is the source of truth;
    the v1 PID files are gone (container PIDs mean nothing on the host)."""
    return compose_service_running("producer")


def write_json_atomic(path: Path, data) -> None:
    """Write JSON so readers never see a half-written file: write a temp file,
    then rename it over the target (rename is atomic on POSIX)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)


# =============================================================================
# Ordering checks
# =============================================================================
@dataclass
class OrderingChecker:
    """Checks the producer-stamped `seq` of every event a consumer sees.

    TraderMade quotes carry no sequence number, so the producer stamps one:
    per symbol, starting at 1, +1 for every published event, reset whenever
    the producer restarts (a new `session_id`). Within one session and one
    symbol, a consumer should therefore see 1, 2, 3, ... with no holes.

    What each outcome means:
        "first"    first event seen for this (session, symbol) — a consumer
                   that starts mid-stream legitimately begins at any seq
        "ok"       exactly the next number
        "gap"      numbers were skipped -> events lost between producer and
                   consumer (expected count: 0)
        "backward" a number at or below the last one -> a duplicate or a
                   reordering. Duplicates are expected after a crash-restart
                   (at-least-once); reordering within a symbol should be 0.
    """

    last_seq: dict[tuple[str, str], int] = field(default_factory=dict)
    gaps: int = 0
    missing: int = 0  # total number of skipped seq values across all gaps
    backward: int = 0

    def observe(self, session_id: str, symbol: str, seq: int) -> str:
        key = (session_id, symbol)
        prev = self.last_seq.get(key)
        if prev is None:
            self.last_seq[key] = seq
            return "first"
        if seq == prev + 1:
            self.last_seq[key] = seq
            return "ok"
        if seq > prev + 1:
            self.gaps += 1
            self.missing += seq - prev - 1
            self.last_seq[key] = seq
            return "gap"
        self.backward += 1  # keep last_seq: the stream is still at `prev`
        return "backward"

    def as_dict(self) -> dict:
        return {"seq_gaps": self.gaps, "seq_missing": self.missing, "seq_backward": self.backward}


# =============================================================================
# Consumer-group lag sampling (used by monitor.py and the experiment runner)
#
# Lag is computed from two numbers per partition:
#   end offset        the offset the NEXT produced message will get
#                     ("high watermark") — asked of the broker
#   committed offset  the offset the group will resume from — stored by the
#                     broker in its internal __consumer_offsets topic
#   lag = end - committed   (messages produced but not yet processed+committed)
# =============================================================================
# A topic-partition, e.g. ("marketdata.fx.quotes.v1", 5). With three topics a
# bare partition number is ambiguous, so every per-partition map uses this.
TP = tuple[str, int]


@dataclass
class GroupSample:
    """One point-in-time view of one consumer group across all its topics."""

    group: str
    state: str  # broker's view: STABLE, EMPTY, PREPARING_REBALANCING, DEAD, UNKNOWN
    members: int  # live consumer processes in the group
    committed: dict[TP, int]  # topic-partition -> committed offset (-1 = never committed)
    end: dict[TP, int]  # topic-partition -> end offset (high watermark)
    # Who owns what, from DescribeGroups: one dict per member with member_id,
    # client_id, host and partitions (a sorted list of [topic, partition]).
    # This is how monitoring sees Storage instances and their rebalances.
    member_details: list[dict] = field(default_factory=list)

    @property
    def committed_total(self) -> int | None:
        """Sum of committed offsets, or None if the group never committed."""
        vals = [c for c in self.committed.values() if c >= 0]
        return sum(vals) if vals else None

    @property
    def end_total(self) -> int | None:
        """Sum of end offsets, or None if any partition's watermark is missing
        (its request failed this sample). A partial sum would look like the
        topic shrank, so we report "unknown" instead."""
        if any(tp not in self.end for tp in self.committed):
            return None
        return sum(self.end.values())

    @property
    def lag(self) -> int | None:
        """Total messages behind across partitions, or None if the group never
        committed or an end offset is missing. Skipping the missing partition
        could understate lag, even show 0, so we report "unknown" instead."""
        if self.committed_total is None or self.end_total is None:
            return None
        return sum(max(0, self.end[tp] - c) for tp, c in self.committed.items() if c >= 0)

    def for_topic(self, topic: str) -> GroupSample:
        """The same sample restricted to one topic, so lag can be reported per
        topic with exactly the same "unknown" rules as the total."""
        return GroupSample(
            self.group,
            self.state,
            self.members,
            {tp: c for tp, c in self.committed.items() if tp[0] == topic},
            {tp: e for tp, e in self.end.items() if tp[0] == topic},
            self.member_details,
        )


def topic_partitions(admin, topics: Iterable[str], timeout: float = 5.0) -> list[TP]:
    """Every (topic, partition) of the given topics, sorted. Raises KeyError
    naming the first topic that does not exist."""
    md = admin.list_topics(timeout=timeout)
    out: list[TP] = []
    for topic in topics:
        t = md.topics.get(topic)
        if t is None or t.error is not None:
            raise KeyError(f"topic {topic!r} missing" + (f": {t.error}" if t else ""))
        out.extend((topic, p) for p in sorted(t.partitions))
    return out


def end_offsets(consumer, tps: Iterable[TP], timeout: float = 5.0) -> dict[TP, int]:
    """High watermark per topic-partition, asked of the broker (cached=False)."""
    from confluent_kafka import TopicPartition

    result = {}
    for topic, p in tps:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=timeout, cached=False
        )
        result[(topic, p)] = high
    return result


# Upper bound on how long one sample_groups() call waits for AdminClient
# answers. `request_timeout` should make librdkafka fail a request on its own,
# but right after `make reset` a request sent while the fresh broker was still
# loading its group coordinator never resolved, and an unbounded
# future.result() froze the monitor forever. This is the backstop.
ADMIN_TIMEOUT_S = 10.0


def _sample_warning(log: logging.Logger | None, event: str, **fields) -> None:
    """Log a sampling problem to the caller's service log if it gave one,
    else to stderr (preflight, experiments)."""
    if log is not None:
        log_event(log, event, level=logging.WARNING, **fields)
    else:
        logging.getLogger(__name__).warning("%s %s", event, fields)


def member_details(desc) -> list[dict]:
    """Members of a ConsumerGroupDescription as plain dicts (sorted by client
    id so consecutive samples compare equal when nothing moved)."""
    out = []
    for m in desc.members:
        tps = m.assignment.topic_partitions if m.assignment is not None else []
        out.append(
            {
                "member_id": m.member_id,
                "client_id": m.client_id,
                "host": m.host,
                "partitions": sorted([tp.topic, tp.partition] for tp in tps),
            }
        )
    return sorted(out, key=lambda d: (d["client_id"], d["member_id"]))


def sample_groups(
    admin,
    consumer,
    groups: Iterable[str],
    tps: list[TP],
    log: logging.Logger | None = None,
    timeout: float = ADMIN_TIMEOUT_S,
) -> list[GroupSample]:
    """Sample end offsets once, then each group's committed offsets, state and
    member assignments, over the topic-partitions `tps`.

    `admin` is a confluent_kafka.admin.AdminClient; `consumer` is any
    Consumer (only used to ask for watermarks). Pass the service logger as
    `log` so warnings land in its JSON log. Without it they go to stderr.

    This function never blocks for long and never raises for one bad request,
    so a broker hiccup costs one sample, not the whole monitor:
      * a failed watermark request leaves that partition out of `end`, so
        that sample's lag and end_total are None (unknown).
      * an error for one group (e.g. it does not exist yet) gives state
        "UNKNOWN" for that group.
      * a group whose admin request has not answered within `timeout`
        seconds gets state "UNKNOWN" and no committed offsets for that sample.
    """
    from concurrent.futures import TimeoutError as FutureTimeout

    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

    groups = list(groups)

    # End offsets, one partition at a time, so one failed watermark request
    # costs one partition rather than the whole sample.
    ends: dict[TP, int] = {}
    for tp in tps:
        try:
            ends.update(end_offsets(consumer, [tp]))
        except Exception as exc:
            _sample_warning(
                log, "end_offset_unavailable", topic=tp[0], partition=tp[1], error=str(exc)
            )

    # Send every request first, then wait on all of them against ONE deadline,
    # so a coordinator that never answers costs `timeout` seconds per sample,
    # not `timeout` per future.
    try:
        describe = admin.describe_consumer_groups(groups, request_timeout=5)
    except Exception as exc:
        _sample_warning(log, "describe_groups_failed", error=str(exc))
        describe = {}
    offset_futs = {}
    for g in groups:
        try:
            # The API takes a list but librdkafka accepts one group per call.
            req = [ConsumerGroupTopicPartitions(g, [TopicPartition(t, p) for t, p in tps])]
            offset_futs[g] = admin.list_consumer_group_offsets(req, request_timeout=5)[g]
        except Exception as exc:
            _sample_warning(log, "list_offsets_failed", group=g, error=str(exc))
    deadline = time.monotonic() + timeout

    samples = []
    for g in groups:
        state, members, details = "UNKNOWN", 0, []
        committed = {tp: -1 for tp in tps}
        timed_out = False
        if g in describe:
            try:
                desc = describe[g].result(timeout=max(0.0, deadline - time.monotonic()))
                state, members = desc.state.name, len(desc.members)
                details = member_details(desc)
            except FutureTimeout:
                timed_out = True
            except Exception:
                pass  # e.g. coordinator error: state stays UNKNOWN
        if g in offset_futs:
            try:
                res = offset_futs[g].result(timeout=max(0.0, deadline - time.monotonic()))
                for tp in res.topic_partitions:
                    committed[(tp.topic, tp.partition)] = tp.offset  # -1001 if none
            except FutureTimeout:
                timed_out = True
            except Exception:
                pass
        if timed_out:
            # The broker did not answer for this group. Report "unknown"
            # rather than mixing a fresh answer with a missing one.
            _sample_warning(log, "group_sample_timeout", group=g, timeout_s=timeout)
            state, members, details = "UNKNOWN", 0, []
            committed = {tp: -1 for tp in tps}
        committed = {tp: (o if o >= 0 else -1) for tp, o in committed.items()}
        samples.append(GroupSample(g, state, members, committed, dict(ends), details))
    return samples


def stall_window_for(group: str, settings: Settings) -> float:
    """How long a group may show no committed progress before we call it
    STALLED. It must be longer than the group's commit cadence, or a healthy
    consumer looks stalled between commits: auto-commit groups commit every
    1 s (window 5 s); Storage commits every STORAGE_OFFSET_COMMIT_INTERVAL_S
    (window 3x that, 15 s by default)."""
    if group == GROUP_STORAGE:
        return max(5.0, 3 * settings.storage_offset_commit_interval_s)
    return 5.0


def derive_status(
    sample: GroupSample,
    history: list[tuple[float, int]],
    now: float,
    stall_window_s: float = 5.0,
) -> str:
    """Summarise a group as RUNNING, STALLED, DOWN or UNKNOWN.

    Args:
        sample:  the latest GroupSample.
        history: (monotonic_time, committed_total) pairs from earlier samples,
                 oldest first. Used to see whether progress is being made.
        now:     monotonic time of `sample`.

    Rules (deliberately simple, so they can be quoted in the article):
        UNKNOWN  the broker did not answer for this group in this sample.
                 Checked first: no answer is not the same as no members.
        DOWN     no live members (group EMPTY or DEAD): nobody is reading.
        RUNNING  caught up (lag 0), or the committed offset moved forward
                 within the last `stall_window_s` seconds.
        STALLED  members present, lag > 0, but no progress for the window
                 (e.g. a consumer stuck in a slow write, or one that was
                 SIGKILLed and has not yet timed out of the group).
    Pass `stall_window_s=stall_window_for(group, settings)` so each group is
    judged against its own commit cadence.
    """
    if sample.state == "UNKNOWN":
        return "UNKNOWN"
    if sample.members == 0 or sample.state in ("EMPTY", "DEAD"):
        return "DOWN"
    if sample.lag == 0:
        return "RUNNING"
    current = sample.committed_total
    recent = [c for (t, c) in history if now - t <= stall_window_s and c is not None]
    if current is not None and recent and current > min(recent):
        return "RUNNING"
    if not recent:
        return "RUNNING"  # not enough history yet to call it stalled
    return "STALLED"
