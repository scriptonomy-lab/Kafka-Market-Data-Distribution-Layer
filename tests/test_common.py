"""Unit tests for the shared helpers: ordering checks, group status rules,
and lag sampling against a broker that does not answer."""

import logging
import time
from concurrent.futures import Future
from types import SimpleNamespace

from pipeline.common import GroupSample, OrderingChecker, derive_status, sample_groups


# ------------------------------------------------------------------ ordering
def test_ordering_checker_counts_gaps_and_backward_steps():
    oc = OrderingChecker()
    assert oc.observe("s1", "EURUSD", 10) == "first"  # mid-stream start is fine
    assert oc.observe("s1", "EURUSD", 11) == "ok"
    assert oc.observe("s1", "EURUSD", 14) == "gap"  # 12 and 13 missing
    assert oc.observe("s1", "EURUSD", 14) == "backward"  # duplicate
    assert oc.observe("s1", "GBPUSD", 1) == "first"  # symbols are independent
    assert oc.observe("s2", "EURUSD", 1) == "first"  # producer restart = new session
    assert oc.as_dict() == {"seq_gaps": 1, "seq_missing": 2, "seq_backward": 1}


# -------------------------------------------------------------- group status
FX, CRYPTO = "marketdata.fx.quotes.v1", "marketdata.crypto.quotes.v1"


def tp(p, topic=FX):
    return (topic, p)


def sample(state="STABLE", members=1, committed=None, end=None):
    committed = committed if committed is not None else {0: 100, 1: 100}
    end = end if end is not None else {0: 100, 1: 100}
    return GroupSample(
        "storage-svc",
        state,
        members,
        {tp(p): c for p, c in committed.items()},
        {tp(p): e for p, e in end.items()},
    )


def test_lag_is_end_minus_committed_summed_over_partitions():
    s = sample(committed={0: 90, 1: 95}, end={0: 100, 1: 100})
    assert s.lag == 15
    assert s.committed_total == 185


def test_lag_can_be_split_per_topic():
    s = GroupSample(
        "storage-svc",
        "STABLE",
        2,
        {tp(0): 90, tp(1): 100, tp(0, CRYPTO): 40},
        {tp(0): 100, tp(1): 100, tp(0, CRYPTO): 50},
    )
    assert s.lag == 20
    assert s.for_topic(FX).lag == 10
    assert s.for_topic(CRYPTO).lag == 10


def test_group_that_never_committed_has_unknown_lag():
    s = sample(committed={0: -1, 1: -1})
    assert s.lag is None and s.committed_total is None


def test_no_members_is_down():
    assert derive_status(sample(state="EMPTY", members=0), [], now=10) == "DOWN"


def test_caught_up_is_running_even_without_progress():
    assert derive_status(sample(), [(5.0, 200), (9.0, 200)], now=10) == "RUNNING"


def test_progress_within_window_is_running():
    s = sample(committed={0: 90, 1: 95})  # lag 15, committed_total 185
    assert derive_status(s, [(6.0, 170), (9.0, 180)], now=10) == "RUNNING"


def test_members_but_no_progress_is_stalled():
    s = sample(committed={0: 90, 1: 95})
    assert derive_status(s, [(6.0, 185), (9.0, 185)], now=10) == "STALLED"


def test_storage_gets_a_longer_stall_window_than_its_commit_interval():
    from pipeline.common import GROUP_PRICING, GROUP_STORAGE, Settings, stall_window_for

    settings = Settings.from_env()
    assert stall_window_for(GROUP_STORAGE, settings) > settings.storage_offset_commit_interval_s
    assert stall_window_for(GROUP_PRICING, settings) == 5.0
    # Between two 5 s commits a healthy Storage must not look STALLED
    s = sample(committed={0: 90, 1: 95})
    history = [(1.0, 180), (6.0, 185), (9.0, 185)]  # last progress at t=6
    assert (
        derive_status(s, history, now=12, stall_window_s=stall_window_for(GROUP_STORAGE, settings))
        == "RUNNING"
    )


# ------------------------------------------------- sampling with a sick broker
# Fakes for AdminClient / Consumer. Futures are real concurrent.futures.Future
# objects, the same type confluent-kafka returns, so .result(timeout=...)
# behaves exactly as in production.
def _done(value) -> Future:
    f = Future()
    f.set_result(value)
    return f


class FakeAdmin:
    """Answers for groups in `healthy`; every other group's futures never
    complete, like a broker still loading its group coordinator."""

    def __init__(self, healthy=()):
        self.healthy = set(healthy)

    def describe_consumer_groups(self, groups, request_timeout):
        member = SimpleNamespace(
            member_id="m1", client_id=f"{groups[0]}-host", host="/10.0.0.1", assignment=None
        )
        return {
            g: _done(SimpleNamespace(state=SimpleNamespace(name="STABLE"), members=[member]))
            if g in self.healthy
            else Future()
            for g in groups
        }

    def list_consumer_group_offsets(self, req, request_timeout):
        g = req[0].group_id
        tps = [
            SimpleNamespace(topic=t.topic, partition=t.partition, offset=90)
            for t in req[0].topic_partitions
        ]
        return {g: _done(SimpleNamespace(topic_partitions=tps)) if g in self.healthy else Future()}


class FakeConsumer:
    """Watermark is 100 everywhere, except partitions in `failing`, whose
    request raises, as a timed-out get_watermark_offsets does."""

    def __init__(self, failing=()):
        self.failing = set(failing)

    def get_watermark_offsets(self, tp, timeout, cached):
        if tp.partition in self.failing:
            raise RuntimeError("watermark request timed out")
        return 0, 100


def test_admin_future_that_never_completes_gives_unknown_instead_of_hanging(caplog):
    admin = FakeAdmin(healthy={"pricing-svc"})
    t0 = time.monotonic()
    with caplog.at_level(logging.WARNING):
        samples = sample_groups(
            admin, FakeConsumer(), ["pricing-svc", "storage-svc"], [tp(0), tp(1)], timeout=0.2
        )
    # Bounded by ONE shared deadline, not one per future
    assert time.monotonic() - t0 < 1.0
    ok, stuck = samples
    assert (ok.state, ok.members, ok.lag) == ("STABLE", 1, 20)  # healthy group unaffected
    assert stuck.state == "UNKNOWN" and stuck.members == 0
    assert stuck.committed == {tp(0): -1, tp(1): -1} and stuck.lag is None
    assert derive_status(stuck, [], now=1.0) == "UNKNOWN"  # not DOWN
    assert "group_sample_timeout" in caplog.text and "storage-svc" in caplog.text


def test_failed_watermark_request_makes_lag_unknown_instead_of_crashing(caplog):
    admin = FakeAdmin(healthy={"storage-svc"})
    with caplog.at_level(logging.WARNING):
        (s,) = sample_groups(admin, FakeConsumer(failing={1}), ["storage-svc"], [tp(0), tp(1)])
    assert s.end == {tp(0): 100}
    assert s.committed == {tp(0): 90, tp(1): 90}  # committed offsets still sampled
    # A partial sum would understate lag, so both totals are unknown
    assert s.lag is None and s.end_total is None
    assert "end_offset_unavailable" in caplog.text
