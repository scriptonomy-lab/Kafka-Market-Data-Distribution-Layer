"""
The experiments' background sampler: what Kafka says about every consumer
group, every `interval` seconds, written to CSV as it happens.

It uses the same function as the monitor (pipeline/common.py:sample_groups),
on its own clients and thread, so a run's numbers never depend on the
monitor container being up. Three files:

    lag.csv                   one row per group x topic per sample (topic "*" =
                              the group's total), with status and phase
    group_states.csv          storage-svc's broker state and member count per
                              sample (PREPARING_REBALANCE / COMPLETING_REBALANCE
                              / STABLE): the rebalance clock
    ownership.csv             storage-svc: for every topic-partition, which
                              instance owned it in that sample ("" = nobody).
                              Experiment F draws its ownership timeline from it.

`phase` is set by the runner (setup, baseline, outage, step-1, ...), so
every row says what was being done to the system when it was taken.
"""

from __future__ import annotations

import csv
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from pipeline.common import (
    ALL_GROUPS,
    GROUP_STORAGE,
    TP,
    Settings,
    derive_status,
    iso_ms,
    sample_groups,
    stall_window_for,
    utc_now,
)
from pipeline.monitor import instance_of

LAG_FIELDS = [
    "ts",
    "elapsed_s",
    "phase",
    "group",
    "topic",
    "status",
    "state",
    "members",
    "committed_total",
    "end_total",
    "lag",
]
STATE_FIELDS = ["ts", "elapsed_s", "phase", "state", "members", "instances"]
OWNER_FIELDS = ["ts", "elapsed_s", "phase", "topic", "partition", "instance"]


class Sampler(threading.Thread):
    def __init__(
        self, settings: Settings, tps: list[TP], out_dir: Path, t0: float, interval: float = 1.0
    ):
        super().__init__(daemon=True)
        self.settings, self.tps, self.t0, self.interval = settings, tps, t0, interval
        self.topics = settings.topology.topic_names
        # Own clients: confluent-kafka handles are not shared across threads here
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        self.probe = Consumer(
            {"bootstrap.servers": settings.kafka_bootstrap, "group.id": "experiment-probe"}
        )
        self.phase = "setup"
        self.rows: list[dict] = []  # lag rows (all groups, all topics)
        self.states: list[dict] = []  # storage-svc state rows
        self.owners: list[dict] = []  # storage-svc ownership rows
        # storage-svc (monotonic t, committed per tp, end per tp): for caught_up()
        self.snapshots: list[tuple[float, dict, dict]] = []
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.history = {g: [] for g in ALL_GROUPS}
        self.files = {}
        self.writers = {}
        for name, fields in (
            ("lag", LAG_FIELDS),
            ("group_states", STATE_FIELDS),
            ("ownership", OWNER_FIELDS),
        ):
            f = open(out_dir / f"{name}.csv", "w", newline="")  # noqa: SIM115 - closed in run()
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            self.files[name], self.writers[name] = f, w

    def run(self) -> None:
        try:
            while not self.stopped.is_set():
                tick = time.monotonic()
                samples = sample_groups(self.admin, self.probe, ALL_GROUPS, self.tps)
                ts = iso_ms(utc_now())
                base = {"ts": ts, "elapsed_s": round(tick - self.t0, 2), "phase": self.phase}
                with self.lock:
                    for s in samples:
                        status = derive_status(
                            s, self.history[s.group], tick, stall_window_for(s.group, self.settings)
                        )
                        self.history[s.group] = (
                            self.history[s.group] + [(tick, s.committed_total)]
                        )[-60:]
                        for topic in ["*", *self.topics]:
                            part = s if topic == "*" else s.for_topic(topic)
                            row = {
                                **base,
                                "group": s.group,
                                "topic": topic,
                                "status": status,
                                "state": s.state,
                                "members": s.members,
                                "committed_total": part.committed_total,
                                "end_total": part.end_total,
                                "lag": part.lag,
                            }
                            self.rows.append(row)
                            self.writers["lag"].writerow(row)
                        if s.group == GROUP_STORAGE:
                            self._record_storage(s, base)
                            if s.state != "UNKNOWN":
                                self.snapshots.append((tick, dict(s.committed), dict(s.end)))
                                self.snapshots = self.snapshots[-600:]
                    for f in self.files.values():
                        f.flush()
                self.stopped.wait(max(0.0, self.interval - (time.monotonic() - tick)))
        finally:
            for f in self.files.values():
                f.close()
            self.probe.close()

    def _record_storage(self, s, base: dict) -> None:
        owner = {}
        for m in s.member_details:
            for t, p in m["partitions"]:
                owner[(t, p)] = instance_of(m["client_id"], GROUP_STORAGE)
        state = {
            **base,
            "state": s.state,
            "members": s.members,
            "instances": "|".join(
                sorted({instance_of(m["client_id"], GROUP_STORAGE) for m in s.member_details})
            ),
        }
        self.states.append({**state, "owner": owner})
        self.writers["group_states"].writerow(state)
        if s.state == "UNKNOWN":
            return  # no answer this sample: not the same as "nobody owns it"
        for t, p in self.tps:
            row = {**base, "topic": t, "partition": p, "instance": owner.get((t, p), "")}
            self.writers["ownership"].writerow(row)

    # ---- queries used by the runners (thread-safe) -------------------------
    def latest(self, group: str, topic: str = "*") -> dict | None:
        with self.lock:
            for row in reversed(self.rows):
                if row["group"] == group and row["topic"] == topic:
                    return row
        return None

    def max_lag(self, group: str, phase: str, topic: str = "*") -> int | None:
        with self.lock:
            lags = [
                r["lag"]
                for r in self.rows
                if r["group"] == group
                and r["topic"] == topic
                and r["phase"] == phase
                and r["lag"] is not None
            ]
        return max(lags) if lags else None

    def caught_up(self, within_s: float) -> bool:
        """Has storage-svc committed, on EVERY topic-partition, everything
        that was in the log `within_s` seconds ago?

        This is "caught up" without a lag threshold. Storage commits offsets
        every few seconds, so its committed lag is a sawtooth whose height
        grows with the message rate and the number of instances (each
        commits on its own clock); a fixed "lag <= N" would be met by chance
        or never. Comparing with the log's end `within_s` seconds earlier
        asks the real question: is it at most `within_s` seconds behind?"""
        with self.lock:
            if not self.snapshots:
                return False
            now, committed, _end = self.snapshots[-1]
            older = [snap for snap in self.snapshots if snap[0] <= now - within_s]
            if not older:
                return False
            _t, _c, end_then = older[-1]
        return all(end == 0 or committed.get(tp, -1) >= end for tp, end in end_then.items())

    def storage_states(self) -> list[dict]:
        with self.lock:
            return list(self.states)
