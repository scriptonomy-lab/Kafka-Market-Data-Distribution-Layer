"""Unit tests for the experiment analysis that README numbers depend on:
when a rebalance counts as done (experiment F), how per-partition pauses are
measured, and what "caught up" means for a sawtooth lag (all runners)."""

import threading
from datetime import UTC, datetime, timedelta

from experiments.sampler import Sampler
from experiments.scale_rebalance import partition_activity, rebalance_done, throughput

T0 = datetime(2026, 11, 16, 15, 0, 0, tzinfo=UTC)
FX0, FX1 = ("fx", 0), ("fx", 1)


def at(s):
    return T0 + timedelta(seconds=s)


def state(s, st, owner):
    return {"ts": at(s).isoformat().replace("+00:00", "Z"), "state": st, "owner": owner}


def test_rebalance_done_needs_stable_and_live_owners_and_a_change():
    before = {FX0: "a", FX1: "b"}
    states = [
        state(1, "STABLE", before),  # b was killed at t=0 but is still listed
        state(5, "PREPARING_REBALANCE", before),
        state(11, "STABLE", {FX0: "a"}),  # FX1 unowned: not done
        state(12, "STABLE", {FX0: "a", FX1: "a"}),  # done
    ]
    done, owner = rebalance_done(states, at(0), live={"a"}, tps=[FX0, FX1], before_owner=before)
    assert done["ts"].startswith("2026-11-16T15:00:12")
    assert owner == {FX0: "a", FX1: "a"}


def test_partition_activity_measures_delay_and_gaps():
    rows = [
        # topic, partition, offset, kafka_ts, stored_at, instance
        ("fx", 0, 1, at(0.0), at(0.2), "a"),
        ("fx", 0, 2, at(1.0), at(1.2), "a"),
        ("fx", 0, 3, at(2.0), at(9.0), "b"),  # paused: written 7 s late by the new owner
    ]
    a = partition_activity(rows, [FX0, FX1], at(0), at(10))
    assert a[FX0]["rows"] == 3
    assert a[FX0]["max_delay_ms"] == 7000.0
    assert a[FX0]["max_gap_ms"] == 7800.0  # 1.2 s -> 9.0 s
    assert a[FX0]["instances"] == ["a", "b"]
    assert a[FX1] == {"rows": 0, "max_delay_ms": None, "max_gap_ms": None, "instances": []}


def test_throughput_buckets_messages_per_instance():
    batches = [
        {"written_at": at(1), "instance_id": "a", "messages": 10},
        {"written_at": at(4), "instance_id": "a", "messages": 15},
        {"written_at": at(6), "instance_id": "b", "messages": 5},
    ]
    assert throughput(batches, T0) == [
        {"t_s": 0.0, "instance": "a", "messages": 25, "msgs_per_s": 5.0},
        {"t_s": 5.0, "instance": "b", "messages": 5, "msgs_per_s": 1.0},
    ]


def bare_sampler(snapshots):
    """A Sampler without threads or Kafka clients, for caught_up() only."""
    s = Sampler.__new__(Sampler)
    s.lock = threading.Lock()
    s.snapshots = snapshots
    return s


def test_caught_up_compares_with_the_log_end_some_seconds_ago():
    tp = ("fx", 5)
    # t=0: log ends at 100. t=10: committed 100, log already at 150 (sawtooth lag 50)
    s = bare_sampler([(0.0, {tp: 40}, {tp: 100}), (10.0, {tp: 100}, {tp: 150})])
    assert s.caught_up(within_s=10)  # everything older than 10 s is committed
    s = bare_sampler([(0.0, {tp: 40}, {tp: 100}), (10.0, {tp: 99}, {tp: 150})])
    assert not s.caught_up(within_s=10)  # still behind the log of 10 s ago
    assert not bare_sampler([(10.0, {tp: 100}, {tp: 150})]).caught_up(within_s=10)  # no history


def test_caught_up_ignores_empty_partitions():
    s = bare_sampler(
        [(0.0, {("fx", 0): -1}, {("fx", 0): 0}), (10.0, {("fx", 0): -1}, {("fx", 0): 0})]
    )
    assert s.caught_up(within_s=10)
