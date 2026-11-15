"""
Protocol tests for pipeline/producer.py against the offline simulator
(scripts/fake_tradermade_server.py). Kafka is replaced by a tiny fake, so
these run anywhere in a few seconds.

They pin down the behaviours the brief lists for the WebSocket side:
authentication, subscription, parsing, connection loss, reconnection and
RE-subscription — plus the duplicate_login and bad-key paths.
"""

import asyncio
import dataclasses
import json

import websockets

from pipeline.common import Settings, get_logger
from pipeline.producer import run
from scripts.fake_tradermade_server import SimConfig, start_server


class FakeMsg:
    def __init__(self, topic: str, key: bytes):
        self._topic, self._key = topic, key

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def partition(self):
        return 0


class FakeKafka:
    """Records produce() calls and 'delivers' them on poll()."""

    def __init__(self):
        self.messages = []  # (key, decoded event)
        self.topics = []  # topic of each message, same order
        self._pending = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((key.decode(), json.loads(value)))
        self.topics.append(topic)
        self._pending.append((topic, key, on_delivery))

    def poll(self, timeout=0):
        pending, self._pending = self._pending, []
        for topic, key, cb in pending:
            cb(None, FakeMsg(topic, key))
        return len(pending)

    def flush(self, timeout=0):
        return self.poll()

    def __len__(self):
        return len(self._pending)


def make_settings(tmp_path, port, key="test-key"):
    base = Settings.from_env()
    return dataclasses.replace(
        base,
        ws_url=f"ws://127.0.0.1:{port}/feedAdv",
        ws_key=key,
        stale_after_s=5,
        ws_ping_interval_s=None,
        log_dir=tmp_path / "logs",
        run_dir=tmp_path / "run",
    )


def log_events(tmp_path, name):
    lines = (tmp_path / "logs" / "producer.log").read_text().splitlines()
    return [json.loads(line) for line in lines if json.loads(line).get("event") == name]


async def run_for(settings, kafka, seconds, during=None):
    """Run the producer for `seconds`; optionally run `during()` meanwhile."""
    stop = asyncio.Event()
    log = get_logger("producer", settings, console=False)
    task = asyncio.create_task(run(settings, kafka, log, stop))
    if during is not None:
        await during()
    await asyncio.sleep(seconds)
    stop.set()
    return await asyncio.wait_for(task, timeout=15)


def start(cfg):
    async def _start():
        server = await start_server("127.0.0.1", 0, cfg)
        return server, server.sockets[0].getsockname()[1]

    return _start


def test_login_subscribe_and_publish(tmp_path):
    async def scenario():
        server, port = await start(SimConfig(rate=20, seed=1))()
        kafka = FakeKafka()
        code = await run_for(make_settings(tmp_path, port), kafka, 1.5)
        server.close()
        return code, kafka

    code, kafka = asyncio.run(scenario())
    assert code == 0
    assert kafka.messages, "no events were published"
    # The first event per symbol is the send_last snapshot
    first_type = {}
    for key, ev in kafka.messages:
        first_type.setdefault(key, ev["type"])
        assert key == ev["symbol"]  # Kafka key = symbol
        assert ev["source"] == "simulated"  # not the official host
    assert set(first_type.values()) == {"snapshot"}
    # seq is 1, 2, 3, ... per symbol
    for sym in first_type:
        seqs = [ev["seq"] for k, ev in kafka.messages if k == sym]
        assert seqs == list(range(1, len(seqs) + 1))
    sub_ack = log_events(tmp_path, "sub_ack")[0]
    # Subscribed with the PROVIDER codes from topology.toml ("AAPL.US")
    topology = Settings.from_env().topology
    assert sorted(sub_ack["accepted"]) == sorted(
        f"{i.ws_symbol}:QUOTE" for i in topology.instruments.values()
    )


def test_each_event_is_routed_to_its_asset_class_topic(tmp_path):
    async def scenario():
        server, port = await start(SimConfig(rate=20, seed=3))()
        kafka = FakeKafka()
        await run_for(make_settings(tmp_path, port), kafka, 1.5)
        server.close()
        return kafka

    kafka = asyncio.run(scenario())
    topology = Settings.from_env().topology
    seen_topics = set()
    for (key, ev), topic in zip(kafka.messages, kafka.topics, strict=True):
        # Topic comes from the configured asset class, key is the internal symbol
        assert topic == topology.topic_for(key)
        assert ev["asset_class"] == topology.instrument(key).asset_class
        seen_topics.add(topic)
    assert seen_topics == set(topology.topic_names)
    # Provider code "AAPL.US" became internal symbol "AAPL"
    assert "AAPL" in {k for k, _ in kafka.messages}
    assert not any("." in k for k, _ in kafka.messages)
    # The observed partition map is kept per topic
    observed = json.loads((tmp_path / "run" / "partition_map.json").read_text())
    assert set(observed) == set(topology.topic_names)
    assert set(observed["marketdata.crypto.quotes.v1"]) == {"BTCUSD", "ETHUSD"}


def test_more_symbols_than_the_plan_allows_is_fatal(tmp_path):
    async def scenario():
        # The plan says 5 symbols; topology.toml has 8
        server, port = await start(SimConfig(symbol_limit=5))()
        kafka = FakeKafka()
        code = await run_for(make_settings(tmp_path, port), kafka, 1.0)
        server.close()
        return code, kafka

    code, kafka = asyncio.run(scenario())
    assert code == 2
    assert not kafka.messages  # never subscribed, nothing published
    assert "allows 5" in log_events(tmp_path, "fatal")[0]["reason"]


def test_reconnects_and_resubscribes_after_connection_loss(tmp_path):
    async def scenario():
        # The simulator drops every connection after 0.7 s
        server, port = await start(SimConfig(rate=20, drop_every=0.7, seed=2))()
        kafka = FakeKafka()
        code = await run_for(make_settings(tmp_path, port), kafka, 4.0)
        server.close()
        return code, kafka

    code, kafka = asyncio.run(scenario())
    assert code == 0
    logins = log_events(tmp_path, "login_ok")
    subs = log_events(tmp_path, "sub_ack")
    assert len(logins) >= 2, "producer did not log in again after the drop"
    assert len(subs) == len(logins), "every new connection must re-subscribe"
    assert log_events(tmp_path, "resubscribed"), "outage window was not logged"
    # Still one session_id and contiguous seq across reconnects
    sessions = {ev["session_id"] for _, ev in kafka.messages}
    assert len(sessions) == 1
    for sym in ("EURUSD", "GBPUSD", "USDJPY"):
        seqs = [ev["seq"] for k, ev in kafka.messages if k == sym]
        assert seqs == list(range(1, len(seqs) + 1))


def test_bad_key_is_fatal_not_retried(tmp_path):
    async def scenario():
        server, port = await start(SimConfig())()
        code = await run_for(make_settings(tmp_path, port, key="bad"), FakeKafka(), 1.0)
        server.close()
        return code

    assert asyncio.run(scenario()) == 2
    assert "login rejected" in log_events(tmp_path, "fatal")[0]["reason"]


def test_duplicate_login_kicks_producer_and_it_backs_off(tmp_path):
    async def scenario():
        server, port = await start(SimConfig(rate=5))()
        settings = make_settings(tmp_path, port)

        async def second_client_logs_in():
            await asyncio.sleep(0.8)
            async with websockets.connect(f"ws://127.0.0.1:{port}/feedAdv") as ws:
                await ws.send(
                    json.dumps({"action": "login", "key": settings.ws_key, "fmt": "JSON"})
                )
                await ws.recv()  # login_ok for the intruder
                await asyncio.sleep(0.5)

        code = await run_for(settings, FakeKafka(), 0.5, during=second_client_logs_in)
        server.close()
        return code

    assert asyncio.run(scenario()) == 0
    reasons = [e["reason"] for e in log_events(tmp_path, "reconnecting")]
    assert any("duplicate_login" in r for r in reasons)
    kicked = [e for e in log_events(tmp_path, "reconnecting") if "duplicate_login" in e["reason"]][
        0
    ]
    assert kicked["delay_s"] >= 30  # long backoff, so two processes do not fight
