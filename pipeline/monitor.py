"""
Monitoring consumer — consumer group "monitor-svc".

Workload: operational visibility for the whole pipeline, as an independent
consumer of the same three topics. Three jobs:

1. As a CONSUMER of the quote streams, it measures what is entering the
   pipeline per topic and symbol: ticks per second and seconds since the
   symbol's last tick (feed freshness). US stocks going quiet at 16:00 New
   York shows up here as growing freshness on the equities topic only.

2. As an OBSERVER of Kafka (AdminClient), once a second it asks the broker,
   for every consumer group: committed offsets, end offsets, group state and
   members. From those it derives lag PER TOPIC, processing rate and a status:
       RUNNING  progressing or caught up
       STALLED  members present but no progress for longer than the group's
                commit cadence allows (5 s; 15 s for Storage)
       DOWN     no members — nobody is reading
       UNKNOWN  the broker did not answer for this group this second
   (rules in pipeline/common.py:derive_status)

3. It watches ASSIGNMENTS: which member (container) owns which
   topic-partitions, from DescribeGroups. When that changes (a Storage
   instance joined, left or died and the group rebalanced) it logs one
   `rebalance_observed` line with the before and after assignment, and
   appends the new assignment to assignments.csv.

Files (in results/monitor/, bind-mounted from the host):
    metrics.csv       every second: one row per group x topic, plus one
                      "*" row per group with the totals
    assignments.csv   the full assignment of a group each time it changes
                      (ts, group, member, instance, topic, partition)

During experiments the runner records its own CSVs with the same sampler,
so charts never depend on this screen having been open.

Monitoring reads Kafka's own bookkeeping, not the other services' logs: it
would keep working even if a service were written in another language.

Run:  docker compose up -d monitor, then  make watch-monitor
"""

from __future__ import annotations

import csv
import sys
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from rich import box
from rich.console import Group
from rich.live import Live
from rich.table import Table

from pipeline.common import (
    ALL_GROUPS,
    GROUP_MONITOR,
    GroupSample,
    Settings,
    StopFlag,
    consumer_config,
    derive_status,
    get_logger,
    iso_ms,
    log_event,
    now_ms,
    sample_groups,
    stall_window_for,
    topic_partitions,
    utc_now,
)
from pipeline.topology import expected_partition_map

SAMPLE_EVERY_S = 1.0
RATE_WINDOW_S = 5.0  # processing rate = committed-offset change over this window
INGRESS_WINDOW_S = 10.0  # ticks/s computed over this window
METRICS_FIELDS = [
    "ts",
    "group",
    "topic",  # "*" = all topics of the group
    "status",
    "state",
    "members",
    "committed_total",
    "end_total",
    "lag",
    "rate_per_s",
]
ASSIGNMENT_FIELDS = ["ts", "group", "member", "instance", "topic", "partition"]
STATUS_STYLE = {"RUNNING": "green", "STALLED": "yellow", "DOWN": "red", "UNKNOWN": "dim"}
SHORT_TOPIC = {"fx": "FX", "crypto": "Crypto", "us_stocks": "Stocks"}


# =============================================================================
# Assignment tracking (pure functions, unit-tested in tests/test_monitor.py)
# =============================================================================
def instance_of(client_id: str, group: str) -> str:
    """Member client.id is "<group>-<instance_id>" (pipeline/common.py), so
    the instance (container hostname) is what follows the group name."""
    prefix = f"{group}-"
    return client_id[len(prefix) :] if client_id.startswith(prefix) else client_id


def assignment_of(sample: GroupSample) -> dict[str, tuple]:
    """instance -> sorted tuple of (topic, partition) it owns right now."""
    return {
        instance_of(m["client_id"], sample.group): tuple(tuple(tp) for tp in m["partitions"])
        for m in sample.member_details
    }


def diff_assignments(before: dict[str, tuple], after: dict[str, tuple]) -> dict:
    """What changed between two assignments of one group:
    joined / left instances, and every topic-partition that changed owner
    (owner None = unassigned)."""
    owner_before = {tp: inst for inst, tps in before.items() for tp in tps}
    owner_after = {tp: inst for inst, tps in after.items() for tp in tps}
    moved = sorted(
        {
            (tp[0], tp[1], owner_before.get(tp), owner_after.get(tp))
            for tp in set(owner_before) | set(owner_after)
            if owner_before.get(tp) != owner_after.get(tp)
        },
        key=lambda m: (m[0], m[1]),
    )
    return {
        "joined": sorted(set(after) - set(before)),
        "left": sorted(set(before) - set(after)),
        "moved": [{"topic": t, "partition": p, "from": a, "to": b} for t, p, a, b in moved],
    }


def processing_rate(history: deque, now: float) -> float | None:
    """Messages committed per second over the last RATE_WINDOW_S seconds."""
    window = [(t, c) for t, c in history if now - t <= RATE_WINDOW_S and c is not None]
    if len(window) < 2 or window[-1][0] == window[0][0]:
        return None
    (t0, c0), (t1, c1) = window[0], window[-1]
    return (c1 - c0) / (t1 - t0)


# =============================================================================
# Display
# =============================================================================
def data_partitions(topology) -> set[tuple[str, int]]:
    """Topic-partitions that carry data: where some symbol's key hashes."""
    return {(t, p) for t, syms in expected_partition_map(topology).items() for p in syms.values()}


def compact_partitions(tps: tuple, topic_label: dict[str, str], with_data: set) -> str:
    """(("marketdata.fx.quotes.v1", 1), ...) -> "FX 1*,2*,3 · Crypto 0*,1"
    (* = a symbol hashes to that partition, so it carries data)."""
    by_topic: dict[str, list[str]] = defaultdict(list)
    for t, p in sorted(tps):
        by_topic[topic_label.get(t, t)].append(f"{p}*" if (t, p) in with_data else str(p))
    return " · ".join(f"{t} {','.join(ps)}" for t, ps in by_topic.items())


def render(rows, per_topic_lag, assignments, ingress, freshness, settings) -> Group:
    topology = settings.topology
    label = {
        t.name: SHORT_TOPIC.get(t.asset_class, t.asset_class) for t in topology.topics.values()
    }
    with_data = data_partitions(topology)

    groups = Table(
        title="Monitoring  ·  consumer groups (lag in messages)",
        title_justify="left",
        box=box.SIMPLE_HEAD,
    )
    cols = ["Group", "Status", "State", "Members", *label.values(), "Lag", "Rate/s"]
    for col in cols:
        groups.add_column(col, justify="left" if col in ("Group", "Status", "State") else "right")
    for r in rows:
        style = STATUS_STYLE.get(r["status"], "")
        lags = per_topic_lag.get(r["group"], {})
        groups.add_row(
            r["group"],
            f"[{style}]{r['status']}[/{style}]",
            r["state"],
            str(r["members"]),
            *["-" if lags.get(t) is None else str(lags.get(t)) for t in label],
            "-" if r["lag"] is None else str(r["lag"]),
            "-" if r["rate_per_s"] is None else f"{r['rate_per_s']:.1f}",
        )

    members = Table(
        title="Members and assigned partitions (* = partition carries data)",
        title_justify="left",
        box=box.SIMPLE_HEAD,
    )
    for col in ("Group", "Instance", "Parts", "Data", "Assigned partitions"):
        members.add_column(col, justify="right" if col in ("Parts", "Data") else "left")
    for group, assignment in assignments.items():
        for inst, tps in sorted(assignment.items()):
            text = compact_partitions(tps, label, with_data)
            members.add_row(group, inst, str(len(tps)), str(len(set(tps) & with_data)), text)

    feed = Table(
        title="Ingress  ·  what TraderMade is sending", title_justify="left", box=box.SIMPLE_HEAD
    )
    for col in ("Topic", "Symbol", "Ticks/s (10 s)", "Since last tick (s)"):
        feed.add_column(col, justify="left" if col in ("Topic", "Symbol") else "right")
    for sym in [s for s in topology.symbols if s in ingress or s in freshness]:
        feed.add_row(
            label[topology.topic_for(sym)],
            sym,
            f"{ingress.get(sym, 0.0):.2f}",
            f"{freshness.get(sym, 0.0):.1f}",
        )
    return Group(groups, members, feed)


# =============================================================================
# Main loop
# =============================================================================
def open_csv(path, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    f = open(path, "a", newline="")  # noqa: SIM115 - closed in main()'s finally
    writer = csv.DictWriter(f, fieldnames=fields)
    if new_file:
        writer.writeheader()
    return f, writer


def main() -> int:
    settings = Settings.from_env()
    log = get_logger("monitor", settings, console=False)
    stop = StopFlag()
    topics = settings.topology.topic_names

    consumer = Consumer(
        consumer_config(settings, GROUP_MONITOR, auto_commit=True, offset_reset="latest")
    )
    consumer.subscribe(topics)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    tps = topic_partitions(admin, topics)

    out_dir = settings.results_dir / "monitor"
    metrics_file, metrics = open_csv(out_dir / "metrics.csv", METRICS_FIELDS)
    assign_file, assign_csv = open_csv(out_dir / "assignments.csv", ASSIGNMENT_FIELDS)

    history = {g: deque(maxlen=120) for g in ALL_GROUPS}  # (monotonic t, committed_total)
    last_assignment: dict[str, dict] = {}  # group -> instance -> partitions
    arrivals: dict[str, deque] = defaultdict(deque)  # symbol -> recent arrival times
    last_tick_ms: dict[str, int] = {}
    last_sample = 0.0
    log_event(log, "started", groups=list(ALL_GROUPS), topics=topics, dir=str(out_dir))

    try:
        # screen=True: repaint the whole pane on the alternate screen each
        # refresh, so no partial old frame survives (see pricing.py).
        with Live(render([], {}, {}, {}, {}, settings), screen=True) as live:
            while not stop.is_set():
                msg = consumer.poll(0.1)
                if msg is not None and not msg.error():
                    sym = msg.key().decode()
                    arrivals[sym].append(time.monotonic())
                    last_tick_ms[sym] = msg.timestamp()[1]

                now = time.monotonic()
                if now - last_sample < SAMPLE_EVERY_S:
                    continue
                last_sample = now

                ts = iso_ms(utc_now())
                rows, per_topic_lag = [], {}
                for s in sample_groups(admin, consumer, ALL_GROUPS, tps, log=log):
                    status = derive_status(
                        s, list(history[s.group]), now, stall_window_for(s.group, settings)
                    )
                    history[s.group].append((now, s.committed_total))
                    row = {
                        "ts": ts,
                        "group": s.group,
                        "topic": "*",
                        "status": status,
                        "state": s.state,
                        "members": s.members,
                        "committed_total": s.committed_total,
                        "end_total": s.end_total,
                        "lag": s.lag,
                        "rate_per_s": processing_rate(history[s.group], now),
                    }
                    rows.append(row)
                    metrics.writerow(row)
                    per_topic_lag[s.group] = {}
                    for t in topics:
                        st = s.for_topic(t)
                        per_topic_lag[s.group][t] = st.lag
                        metrics.writerow(
                            {
                                **row,
                                "topic": t,
                                "committed_total": st.committed_total,
                                "end_total": st.end_total,
                                "lag": st.lag,
                                "rate_per_s": None,
                            }
                        )

                    # Assignment changes. A sample with state UNKNOWN carries no
                    # members (the broker did not answer): not a rebalance.
                    if s.state == "UNKNOWN":
                        continue
                    current = assignment_of(s)
                    previous = last_assignment.get(s.group)
                    if current != previous:
                        if previous is not None:
                            log_event(
                                log,
                                "rebalance_observed",
                                group=s.group,
                                state=s.state,
                                before={i: [list(tp) for tp in p] for i, p in previous.items()},
                                after={i: [list(tp) for tp in p] for i, p in current.items()},
                                **diff_assignments(previous, current),
                            )
                        for m in s.member_details:
                            for topic, partition in m["partitions"]:
                                assign_csv.writerow(
                                    {
                                        "ts": ts,
                                        "group": s.group,
                                        "member": m["member_id"],
                                        "instance": instance_of(m["client_id"], s.group),
                                        "topic": topic,
                                        "partition": partition,
                                    }
                                )
                        if not s.member_details:  # record "nobody owns anything" too
                            assign_csv.writerow(
                                {
                                    "ts": ts,
                                    "group": s.group,
                                    "member": "",
                                    "instance": "",
                                    "topic": "",
                                    "partition": "",
                                }
                            )
                        last_assignment[s.group] = current
                metrics_file.flush()
                assign_file.flush()

                # Ingress: drop arrivals older than the window, then count
                ingress = {}
                for sym, q in arrivals.items():
                    while q and now - q[0] > INGRESS_WINDOW_S:
                        q.popleft()
                    ingress[sym] = len(q) / INGRESS_WINDOW_S
                freshness = {sym: (now_ms() - t) / 1000 for sym, t in last_tick_ms.items()}
                live.update(
                    render(rows, per_topic_lag, last_assignment, ingress, freshness, settings)
                )
    finally:
        # Also logged when rich's screen restore raises on a closed terminal,
        # so every stop leaves a record.
        consumer.close()
        metrics_file.close()
        assign_file.close()
        log_event(log, "stopped", signal=stop.signal_name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
