"""
TraderMade WebSocket V2  ->  normalize  ->  Kafka.

This is the ONLY process that talks to TraderMade's streaming API. That is not
just a design preference: TraderMade logs out a second session that uses the
same key ("duplicate_login"), so an institution cannot let every service open
its own connection. One ingestion point + Kafka is the natural shape.

Flow of one connection ("session")
----------------------------------
    connect wss://stream.tradermade.com/feedAdv
      -> send  {"action":"login","key":...,"fmt":"JSON"}
      <- recv  {"type":"login_ok", ...}                  (or login_reject: fatal)
      -> send  {"action":"subscribe","send_last":true,"symbols":["EURUSD:QUOTE","AAPL.US:QUOTE",...]}
      <- recv  {"type":"sub_ack","accepted":[...],...}    (anything missing: fatal)
      <- recv  LAST_QUOTE x N (cached snapshot), then QUOTE ticks forever
         each tick: normalize -> stamp seq -> produce(topic of its asset class, key=symbol)

One session, three topics
-------------------------
Still ONE WebSocket session for every symbol (one session per key), but each
event goes to the topic of its asset class: FX, crypto or US stocks
(config/topology.toml). The topic is looked up from the instrument's
configured asset class, never inferred from what the symbol looks like.
Before subscribing, the symbol count is checked against login_ok's
symbol_limit, so a too-large universe fails with a clear message instead of
a partial sub_ack.

When the connection drops (network blip, server restart, stale feed), the
outer loop reconnects with exponential backoff + jitter and REPEATS login and
subscribe: TraderMade does not persist subscriptions across connections.

Why a slow consumer can never slow this process down
-----------------------------------------------------
`Producer.produce()` only appends to librdkafka's in-memory queue and returns
immediately; a background thread ships batches to the broker. Consumers read
from the broker at their own pace. Nothing downstream can apply back-pressure
to the WebSocket read loop. That property is the core argument of the article.

Run:  make producer      (or: python -m pipeline.producer)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import sys
import time
import uuid
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from pipeline.common import (
    Settings,
    get_logger,
    iso_ms,
    log_event,
    producer_config,
    utc_now,
    write_json_atomic,
)
from pipeline.normalize import masked_control, message_kind, normalize_quote, split_frame

# Hostname of the real feed. Any other host (e.g. the offline simulator)
# makes every event carry source="simulated", so dev data can never be
# mistaken for evidence.
OFFICIAL_WS_HOST = "stream.tradermade.com"

BACKOFF_START_S = 1.0  # first reconnect delay
BACKOFF_MAX_S = 30.0  # ceiling for the doubling backoff
KICKED_BACKOFF_S = 30.0  # after duplicate_login: give the other session room
CONTROL_TIMEOUT_S = 10.0  # max wait for login_ok / sub_ack
METRICS_EVERY_S = 10.0  # cadence of the "metrics" log line


# =============================================================================
# Exceptions that steer the reconnect loop
# =============================================================================
class FatalFeedError(Exception):
    """Retrying cannot help (bad key, symbols not on the plan). Exit."""


class SessionKicked(Exception):
    """Server sent logout/duplicate_login: another process uses our key."""


class FeedStale(Exception):
    """No frame for STALE_AFTER_S seconds; the connection is presumed dead."""


class RetryAfter(Exception):
    """Server asked us to back off for a given time (rate limit)."""

    def __init__(self, seconds: float) -> None:
        super().__init__(f"server asked to retry after {seconds:.1f}s")
        self.seconds = seconds


# =============================================================================
# Publisher: turns accepted quotes into Kafka messages. Holds all per-process
# state (sequence numbers, counters) so the async loop below stays readable.
# =============================================================================
class Publisher:
    def __init__(self, kafka: Producer, settings: Settings, log: logging.Logger, source: str):
        self.kafka = kafka
        self.topology = settings.topology
        # Provider code ("AAPL.US") -> instrument; the normalizer's lookup table
        self.instruments = settings.topology.by_ws_symbol
        self.log = log
        self.source = source
        # New on every process start, so consumers can tell a seq reset
        # (producer restart) apart from lost events.
        self.session_id = uuid.uuid4().hex[:12]
        self.seq: Counter[str] = Counter()  # symbol -> last stamped seq
        self.last_published: dict[str, tuple] = {}  # symbol -> (bid, ask, timestamp)
        # topic -> symbol -> partition, from delivery reports
        self.partition_of: dict[str, dict[str, int]] = {}
        self.partition_map_path = settings.run_dir / "partition_map.json"
        self.counts: Counter[str] = Counter()  # received, published, delivered, ...
        self.rejects: Counter[str] = Counter()  # reject reason -> count
        self.window: Counter[str] = Counter()  # symbol -> ticks since last metrics line
        self.by_topic: Counter[str] = Counter()  # topic -> events published
        self.last_tick_monotonic: float | None = None

    def handle(self, msg: dict, ingest_dt: datetime) -> None:
        """Normalize one quote/snapshot and publish it (or count why not)."""
        self.counts["received"] += 1
        event, reason = normalize_quote(msg, self.instruments)
        if event is None:
            self.rejects[reason] += 1
            return

        symbol = event["symbol"]
        fingerprint = (event["bid"], event["ask"], event["timestamp"])
        # After a reconnect, send_last replays the last cached tick. If it is
        # identical to what we already published, publishing it again would
        # be a duplicate, so drop it. A *different* snapshot is real news
        # (the market moved while we were disconnected) and is published.
        if event["type"] == "snapshot" and self.last_published.get(symbol) == fingerprint:
            self.counts["snapshots_dropped"] += 1
            return
        if event["bid"] == event["ask"]:
            self.counts["locked_quotes"] += 1  # allowed, but worth knowing

        # Pipeline fields, added here because they depend on producer state
        self.seq[symbol] += 1
        event["ingest_ts"] = iso_ms(ingest_dt)  # when THIS process received it
        event["seq"] = self.seq[symbol]
        event["session_id"] = self.session_id
        event["source"] = self.source

        # Route by the instrument's configured asset class (topology.toml).
        topic = self.topology.topic_for(symbol)
        # Compact JSON: no spaces, smaller messages
        self._produce(topic, symbol, json.dumps(event, separators=(",", ":")).encode())
        self.by_topic[topic] += 1
        self.last_published[symbol] = fingerprint
        self.last_tick_monotonic = time.monotonic()
        self.counts["published"] += 1
        self.window[symbol] += 1

    def _produce(self, topic: str, symbol: str, value: bytes) -> None:
        """Hand one message to librdkafka. The KEY is the symbol: Kafka hashes
        it to choose the partition, so all ticks of one symbol share one
        partition and therefore keep their order."""
        for _attempt in range(3):
            try:
                self.kafka.produce(
                    topic, key=symbol.encode(), value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # The local queue is full (broker down or very slow). Serving
                # delivery reports for up to 0.5 s frees space. This blocks the
                # event loop briefly — acceptable on a path that should never
                # happen in normal operation.
                self.kafka.poll(0.5)
        else:
            # The event got a seq but never reached Kafka: consumers will
            # report a seq gap, which is exactly the honest outcome.
            self.counts["dropped_queue_full"] += 1
            log_event(self.log, "dropped_queue_full", logging.ERROR, topic=topic, symbol=symbol)
        # Serve any delivery reports that are ready, without waiting
        self.kafka.poll(0)

    def _on_delivery(self, err, msg) -> None:
        """Called by librdkafka (inside poll/flush) once per message."""
        if err is not None:
            self.counts["delivery_failed"] += 1
            log_event(self.log, "delivery_failed", logging.ERROR, error=str(err))
            return
        self.counts["delivered"] += 1
        symbol, topic = msg.key().decode(), msg.topic()
        if symbol not in self.partition_of.get(topic, {}):
            # First successful delivery for this symbol: record where its key
            # hashed to. This is the observed symbol -> partition map, which
            # `make partition-map ARGS=--check` compares with the computed one.
            self.partition_of.setdefault(topic, {})[symbol] = msg.partition()
            log_event(
                self.log, "partition_map", topic=topic, symbol=symbol, partition=msg.partition()
            )
            write_json_atomic(self.partition_map_path, self.partition_of)

    def metrics(self, elapsed_s: float) -> dict:
        """Counters for the periodic metrics line; resets the rate window."""
        rates = {s: round(n / elapsed_s, 2) for s, n in sorted(self.window.items())}
        self.window.clear()
        return {
            **dict(self.counts),
            "rejects": dict(self.rejects),
            "ticks_per_s": rates,
            "published_by_topic": dict(self.by_topic),
            # Messages accepted by produce() but not yet acknowledged
            "kafka_queue": len(self.kafka),
            "session_id": self.session_id,
        }


# =============================================================================
# Control-message handling
# =============================================================================
def handle_control(msg: dict, log: logging.Logger) -> None:
    """React to a control message that arrived when we were not waiting for
    it. Raises to steer the reconnect loop; otherwise just logs."""
    mtype = msg.get("type")
    if mtype == "login_reject":
        raise FatalFeedError(f"login rejected: {msg.get('reason')}")
    if mtype == "logout":
        raise SessionKicked(str(msg.get("reason")))
    if mtype == "error":
        if "retry_after_ms" in msg:
            raise RetryAfter(float(msg["retry_after_ms"]) / 1000.0)
        # e.g. unknown_action / invalid_json: our bug, worth seeing, not fatal
        log_event(log, "server_error", logging.WARNING, **masked_control(msg))
        return
    log_event(log, "control_message", **masked_control(msg))


def dispatch(frame_msgs: list[dict], ingest_dt: datetime, publisher: Publisher, log) -> None:
    """Route every message of one frame to the right handler."""
    for msg in frame_msgs:
        kind = message_kind(msg)
        if kind in ("quote", "snapshot"):
            publisher.handle(msg, ingest_dt)
        elif kind == "control":
            handle_control(msg, log)
        else:
            publisher.rejects["unknown_message"] += 1


async def wait_for_control(ws, wanted: str, publisher: Publisher, log) -> dict:
    """Read frames until the control message `wanted` arrives; anything else
    that arrives meanwhile (e.g. LAST_QUOTE ticks) is handled normally."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CONTROL_TIMEOUT_S
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"no {wanted} within {CONTROL_TIMEOUT_S:.0f}s")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        ingest_dt = utc_now()
        for msg in split_frame(raw):
            if message_kind(msg) == "control" and msg.get("type") == wanted:
                return msg
            dispatch([msg], ingest_dt, publisher, log)


# =============================================================================
# One connection: login, subscribe, stream until something breaks
# =============================================================================
async def run_session(ws, settings: Settings, publisher: Publisher, log, stop: asyncio.Event):
    loop = asyncio.get_running_loop()

    # 1. Login. The key goes over the wire but is never logged.
    await ws.send(json.dumps({"action": "login", "key": settings.ws_key, "fmt": "JSON"}))
    login_ok = await wait_for_control(ws, "login_ok", publisher, log)
    log_event(log, "login_ok", **masked_control(login_ok))

    # 2. Subscribe. "<code>:QUOTE" is the documented form; it makes our
    # request strings match the ack strings byte for byte. send_last=true
    # asks for one cached tick per symbol so consumers get a price at once.
    wanted = [f"{code}:QUOTE" for code in publisher.instruments]
    limit = login_ok.get("symbol_limit")
    if isinstance(limit, int) and len(wanted) > limit:
        # Fail before subscribing: a partial universe would look like a quiet
        # market for the missing symbols, which is worse than not starting.
        raise FatalFeedError(
            f"{len(wanted)} symbols configured in topology.toml but this TraderMade plan "
            f"allows {limit} per session (login_ok symbol_limit); remove symbols"
        )
    await ws.send(json.dumps({"action": "subscribe", "send_last": True, "symbols": wanted}))
    ack = await wait_for_control(ws, "sub_ack", publisher, log)
    log_event(
        log,
        "sub_ack",
        accepted=ack.get("accepted"),
        denied=ack.get("denied"),
        denied_reasons=ack.get("denied_reasons"),
        invalid=ack.get("invalid"),
    )
    missing = sorted(set(wanted) - set(ack.get("accepted") or []))
    if missing:
        raise FatalFeedError(
            f"symbols not accepted: {missing} (reasons: {ack.get('denied_reasons')})"
        )
    publisher.counts["sessions_established"] += 1

    # How long were we deaf? (time since the last published tick, if any)
    if publisher.last_tick_monotonic is not None:
        outage = time.monotonic() - publisher.last_tick_monotonic
        log_event(log, "resubscribed", outage_s=round(outage, 2))

    # 3. Stream. Wake at least once a second to check the stop flag, serve
    # Kafka delivery reports and run the staleness watchdog.
    last_frame = loop.time()
    while not stop.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
        except TimeoutError:
            publisher.kafka.poll(0)
            if loop.time() - last_frame > settings.stale_after_s:
                raise FeedStale(f"no frame for {settings.stale_after_s:.0f}s") from None
            continue
        last_frame = loop.time()
        dispatch(split_frame(raw), utc_now(), publisher, log)


# =============================================================================
# The reconnect loop
# =============================================================================
async def metrics_loop(publisher: Publisher, log, stop: asyncio.Event) -> None:
    """Every METRICS_EVERY_S, write one "metrics" line (counts, rates, queue)."""
    last = time.monotonic()
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=METRICS_EVERY_S)
        except TimeoutError:
            pass
        now = time.monotonic()
        publisher.kafka.poll(0)
        log_event(log, "metrics", **publisher.metrics(max(now - last, 1e-6)))
        last = now


async def run(settings: Settings, kafka: Producer, log: logging.Logger, stop: asyncio.Event) -> int:
    """Run until `stop` is set or a fatal error occurs. Returns an exit code."""
    host = urlparse(settings.ws_url).hostname or ""
    source = "tradermade" if host == OFFICIAL_WS_HOST else "simulated"
    publisher = Publisher(kafka, settings, log, source)
    log_event(
        log,
        "producer_start",
        session_id=publisher.session_id,
        ws_host=host,
        source=source,
        symbols=list(settings.topology.symbols),
        topics={t.name: list(t.symbols) for t in settings.topology.topics.values()},
    )
    if source == "simulated":
        log_event(log, "SIMULATED_FEED", logging.WARNING, note="dev only - not evidence")

    metrics_task = asyncio.create_task(metrics_loop(publisher, log, stop))
    backoff = BACKOFF_START_S
    attempt = 0
    exit_code = 0
    try:
        while not stop.is_set():
            attempt += 1
            established_before = publisher.counts["sessions_established"]
            reason, delay, server_dictated = "", backoff, False
            try:
                async with connect(
                    settings.ws_url,
                    open_timeout=10,
                    ping_interval=settings.ws_ping_interval_s,
                    ping_timeout=settings.ws_ping_interval_s,
                ) as ws:
                    log_event(log, "connected", attempt=attempt)
                    await run_session(ws, settings, publisher, log, stop)
                    # run_session returns normally only when stop was requested
            except FatalFeedError as exc:
                log_event(log, "fatal", logging.ERROR, reason=str(exc))
                exit_code = 2
                break
            except InvalidURI as exc:
                log_event(log, "fatal", logging.ERROR, reason=f"bad TRADERMADE_WS_URL: {exc}")
                exit_code = 2
                break
            except SessionKicked as exc:
                reason, delay, server_dictated = f"logout: {exc}", KICKED_BACKOFF_S, True
            except RetryAfter as exc:
                reason, delay, server_dictated = str(exc), exc.seconds, True
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError, FeedStale) as exc:
                reason = f"{type(exc).__name__}: {exc}"

            if stop.is_set():
                break
            if publisher.counts["sessions_established"] > established_before:
                # The session got as far as sub_ack, so it was healthy before
                # it dropped: reconnect quickly and start the backoff over.
                backoff = BACKOFF_START_S
                if not server_dictated:
                    delay = BACKOFF_START_S
            else:
                # Failing before sub_ack again and again: double the wait
                # each time (1, 2, 4, ... 30 s) so we never hammer the server.
                backoff = min(backoff * 2, BACKOFF_MAX_S)
            delay += random.uniform(0, 1.0)  # jitter: avoid reconnect stampedes
            publisher.counts["reconnects"] += 1
            log_event(log, "reconnecting", logging.WARNING, reason=reason, delay_s=round(delay, 2))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass
    finally:
        stop.set()
        await metrics_task
    return exit_code


def main() -> int:
    settings = Settings.from_env()
    log = get_logger("producer", settings)
    if not settings.ws_key:
        log_event(log, "fatal", logging.ERROR, reason="TRADERMADE_WS_KEY is empty - fill in .env")
        return 2

    kafka = Producer(producer_config(settings))

    async def _amain() -> int:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        # Ctrl-C / SIGTERM set the flag; loops notice it within ~1 s. SIGHUP
        # too: `tmux kill-session` sends it, and by default it kills Python
        # before kafka.flush() can deliver the queued ticks (see StopFlag).
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            loop.add_signal_handler(sig, stop.set)
        return await run(settings, kafka, log, stop)

    code = asyncio.run(_amain())
    # Deliver everything still queued before exiting (up to 10 s).
    undelivered = kafka.flush(10)
    log_event(log, "shutdown", undelivered=undelivered)
    return code


if __name__ == "__main__":
    sys.exit(main())
