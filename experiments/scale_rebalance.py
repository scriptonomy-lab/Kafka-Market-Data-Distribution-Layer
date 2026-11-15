"""
Experiment F — scale-out and rebalance of the Storage group, on live data.

The question: when a consumer group grows or shrinks under live traffic, what
does it cost? How long until every partition has a live owner again, how long
are the moved partitions paused, do the partitions that did NOT move keep
being processed (the promise of the cooperative-sticky assignor), and is
anything lost or written twice?

    make experiment-f          (default 120 s per step, about 10 minutes)

Steps (each runs --step seconds; every docker action is timestamped)
    1. steady      2 Storage instances, nothing happens: the baseline
    2. scale_out   `docker compose up -d --scale storage=3 --no-recreate`
    3. clean_stop  `docker stop` one of the original instances
    4. crash       `docker kill -s KILL` the other original instance; the
                   survivor takes its partitions after session.timeout.ms (10 s)

Measured per step (all from files in the run folder)
    rebalance duration   action start -> first sample where the group is STABLE
                         and every topic-partition is owned by a LIVE instance
                         (a SIGKILLed member stays in the group, owning its
                         partitions, until the broker times it out: those do
                         not count as owned)
    first write          action start -> first row written by the new owner
                         of each moved partition that carries data
    processing per       in the 60 s after the action, for every partition
    partition            that carries data: the largest processing delay
                         (stored_at - Kafka timestamp) and the longest gap
                         between consecutive writes, against the same numbers
                         in the steady step. A partition that paused shows a
                         delay far above its steady value. A gap alone cannot
                         tell a pause from a quiet symbol; the delay can,
                         because it is measured on the messages themselves.
    duplicates           storage_batches.duplicates per step: 0 expected for
                         the clean steps; the crash step's count is reported
    throughput           messages written per instance per 5 s
    no loss              every offset of every topic-partition produced during
                         the run is in TimescaleDB, and 0 seq gaps

Files: actions.json, events.json, lag.csv, group_states.csv, ownership.csv,
storage_batches.csv, partition_activity.csv, throughput.csv,
describe_<n>_<step>_{before,after}.txt, summary.json/.md, and the charts
ownership.png, lag_by_topic.png, throughput.png.

Simulated feeds are never evidence (results/_dev/, labelled SIMULATED).
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from experiments import containers, db_checks
from experiments.failure_recovery import parse_iso, say, short, wait_until
from experiments.sampler import Sampler
from pipeline import rest
from pipeline.common import (
    GROUP_STORAGE,
    REPO_ROOT,
    Settings,
    end_offsets,
    iso_ms,
    sample_groups,
    topic_partitions,
    utc_now,
    write_json_atomic,
)
from pipeline.monitor import data_partitions
from pipeline.producer import OFFICIAL_WS_HOST

STEPS = ("steady", "scale_out", "clean_stop", "crash")
CLEAN_STEPS = ("steady", "scale_out", "clean_stop")
ANALYSIS_WINDOW_S = 60.0  # per-partition pause analysis after each action
THROUGHPUT_BUCKET_S = 5.0


# =============================================================================
# Analysis helpers (pure: inputs are rows already read from files / the DB)
# =============================================================================
def rebalance_done(states: list[dict], since: datetime, live: set[str], tps, before_owner: dict):
    """First sampler state at/after `since` where the group is STABLE, every
    topic-partition is owned by a live instance and the ownership differs
    from `before_owner`. Returns (state_row, owner) or (None, None)."""
    for s in states:
        if parse_iso(s["ts"]) < since or s["state"] != "STABLE":
            continue
        owner = s["owner"]
        if all(owner.get(tp) in live for tp in tps) and owner != before_owner:
            return s, owner
    return None, None


def last_owner_before(states: list[dict], when: datetime) -> dict:
    owner = {}
    for s in states:
        if parse_iso(s["ts"]) >= when:
            break
        if s["state"] != "UNKNOWN":
            owner = s["owner"]
    return owner


def partition_activity(rows, tps_with_data, since: datetime, until: datetime) -> dict:
    """Per topic-partition, over rows WRITTEN in [since, until):
    rows, max processing delay (stored_at - kafka_ts, ms) and the longest gap
    between consecutive writes (ms, including from `since` to the first write)."""
    by_tp = defaultdict(list)
    for topic, partition, _off, kafka_ts, stored_at, instance in rows:
        if since <= stored_at < until:
            by_tp[(topic, partition)].append((stored_at, kafka_ts, instance))
    out = {}
    for tp in tps_with_data:
        items = sorted(by_tp.get(tp, []))
        if not items:
            out[tp] = {"rows": 0, "max_delay_ms": None, "max_gap_ms": None, "instances": []}
            continue
        delays = [(s - k).total_seconds() * 1000 for s, k, _ in items]
        times = [since] + [s for s, _, _ in items]
        gaps = [(b - a).total_seconds() * 1000 for a, b in zip(times, times[1:], strict=False)]
        out[tp] = {
            "rows": len(items),
            "max_delay_ms": round(max(delays), 1),
            "max_gap_ms": round(max(gaps), 1),
            "instances": sorted({i for _, _, i in items}),
        }
    return out


def first_write_by(rows, tp, instance: str, since: datetime) -> datetime | None:
    for topic, partition, _off, _k, stored_at, inst in rows:
        if (topic, partition) == tp and inst == instance and stored_at >= since:
            return stored_at
    return None


def throughput(batches, t0: datetime) -> list[dict]:
    """Messages written per instance per THROUGHPUT_BUCKET_S seconds."""
    buckets: dict[tuple, int] = defaultdict(int)
    for b in batches:
        k = int((b["written_at"] - t0).total_seconds() // THROUGHPUT_BUCKET_S)
        buckets[(k, b["instance_id"])] += b["messages"]
    return [
        {
            "t_s": k * THROUGHPUT_BUCKET_S,
            "instance": inst,
            "messages": n,
            "msgs_per_s": n / THROUGHPUT_BUCKET_S,
        }
        for (k, inst), n in sorted(buckets.items())
    ]


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="Experiment F: Storage scale-out and rebalance")
    ap.add_argument("--step", type=float, default=120, help="seconds per step")
    ap.add_argument("--catchup-timeout", type=float, default=600)
    ap.add_argument(
        "--behind-s",
        type=float,
        default=0,
        help="caught up = committed everything older than this; 0 = commit interval + 5 s",
    )
    ap.add_argument("--skip-market-check", action="store_true", help="DEV ONLY")
    args = ap.parse_args()

    settings = Settings.from_env()
    ws_url = containers.producer_ws_url()
    simulated_url = urlparse(ws_url).hostname != OFFICIAL_WS_HOST
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-expF-scale"
    out_dir = settings.results_dir / ("_dev" if simulated_url else "") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    say(f"Experiment F (scale-out and rebalance) -> {out_dir.relative_to(REPO_ROOT)}")

    if not args.skip_market_check:
        try:
            if not rest.fx_is_open(settings):
                say("FX market is closed: refusing to record evidence on an idle feed.")
                return 2
        except rest.RestError as exc:
            say(f"Market check failed ({exc}); use --skip-market-check only for dev runs.")
            return 2
    for svc in ("producer", "pricing", "risk", "monitor"):
        if not containers.running(svc):
            say(f"Service {svc} is not running. Start the stack first: make up")
            return 2

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    probe = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap, "group.id": "experiment-probe"}
    )
    topics = settings.topology.topic_names
    tps = topic_partitions(admin, topics)
    with_data = sorted(data_partitions(settings.topology))
    actions = containers.Actions()
    events: dict[str, str] = {}
    t_mono0 = time.monotonic()

    # Caught up = committed everything that was in the log this long ago
    behind_s = args.behind_s or settings.storage_offset_commit_interval_s + 5

    # ---- setup: exactly 2 instances, caught up --------------------------------
    if len(containers.storage_instances()) != 2:
        actions.scale(2)
    sampler = Sampler(settings, tps, out_dir, t_mono0, interval=0.5)
    sampler.start()

    def phase(name: str) -> None:
        sampler.phase = name
        events[name] = iso_ms(utc_now())
        say(f"--- phase: {name}")

    step_records = {}
    try:
        phase("setup")
        wait_until(
            lambda: (
                (r := sampler.latest(GROUP_STORAGE)) is not None
                and r["members"] == 2
                and r["state"] == "STABLE"
                and sampler.caught_up(behind_s)
            ),
            timeout=args.catchup_timeout,
            what="2 Storage instances, STABLE and caught up",
        )
        original = containers.storage_instances()
        say(f"Storage instances: {[c['instance'] for c in original]}")
        run_start = utc_now()
        start_offsets = end_offsets(probe, tps)

        for i, step in enumerate(STEPS, start=1):
            before_running = {c["instance"] for c in containers.storage_instances()}
            containers.capture_describe(
                out_dir / f"describe_{i}_{step}_before.txt", f"before {step}"
            )
            phase(step)
            t_step = time.monotonic()
            rec = None
            if step == "scale_out":
                rec = actions.scale(3)
            elif step == "clean_stop":
                rec = actions.stop(original[0]["name"])
            elif step == "crash":
                rec = actions.kill(original[1]["name"])
            live = {c["instance"] for c in containers.storage_instances()}
            step_records[step] = {
                "action": rec,
                "running_before": sorted(before_running),
                "running_after": sorted(live),
            }
            time.sleep(max(0.0, args.step - (time.monotonic() - t_step)))
            containers.capture_describe(out_dir / f"describe_{i}_{step}_after.txt", f"after {step}")

        end_final = end_offsets(probe, tps)
        phase("verify")

        def committed_past_range() -> bool:
            s = sample_groups(admin, probe, [GROUP_STORAGE], tps)[0]
            return all(o == 0 or s.committed.get(tp, -1) >= o for tp, o in end_final.items())

        wait_until(
            committed_past_range, timeout=180, what="Storage to commit past the verification range"
        )
    finally:
        sampler.stopped.set()
        sampler.join(timeout=10)
        probe.close()
        actions.save(out_dir / "actions.json")
        write_json_atomic(out_dir / "events.json", events)

    victims = {"clean_stop": original[0], "crash": original[1]}
    not_restarted = {
        step: {"container": v["name"], **containers.container_state(v["name"])}
        for step, v in victims.items()
    }

    # ---- analysis -------------------------------------------------------------
    states = sampler.storage_states()
    run_end = utc_now()
    with db_checks.connect(settings) as conn:
        db = db_checks.verify_range(conn, start_offsets, end_final)
        batches = db_checks.batches_since(conn, run_start)
        rows = db_checks.rows_in_window(conn, run_start, run_end)
    db_checks.write_batches_csv(batches, out_dir / "storage_batches.csv")
    simulated = simulated_url or "simulated" in db["sources"]

    steady_start = parse_iso(events["steady"])
    steady_activity = partition_activity(
        rows, with_data, steady_start, steady_start + timedelta(seconds=args.step)
    )
    activity_rows = []
    for tp, a in steady_activity.items():
        activity_rows.append(
            {"step": "steady", "topic": tp[0], "partition": tp[1], "moved": False, **a}
        )

    step_results = {}
    for step in STEPS:
        start = parse_iso(events[step])
        end = start + timedelta(seconds=args.step)
        in_step = [b for b in batches if start <= b["written_at"] < end]
        res = {
            "started_at": events[step],
            "duplicates": sum(b["duplicates"] for b in in_step),
            "messages_written": sum(b["messages"] for b in in_step),
            "instances_writing": sorted({b["instance_id"] for b in in_step}),
        }
        if step != "steady":
            act = step_records[step]["action"]
            t_action = parse_iso(act["started_at"])
            live = set(step_records[step]["running_after"])
            before_owner = last_owner_before(states, t_action)
            done, after_owner = rebalance_done(states, t_action, live, tps, before_owner)
            moved = sorted(tp for tp in tps if before_owner.get(tp) != (after_owner or {}).get(tp))
            moved_data = [tp for tp in moved if tp in with_data]
            first_writes = {}
            for tp in moved_data:
                w = first_write_by(rows, tp, after_owner[tp], t_action) if after_owner else None
                first_writes[f"{tp[0]}:{tp[1]}"] = (
                    round((w - t_action).total_seconds(), 3) if w else None
                )
            # Never look past this step: the next step's rebalance would be
            # attributed to this one (seen in a 40 s-step rehearsal).
            window_s = min(ANALYSIS_WINDOW_S, args.step)
            activity = partition_activity(
                rows, with_data, t_action, t_action + timedelta(seconds=window_s)
            )
            # When did the GROUP start rebalancing? The first rebalance
            # callback any instance logged after the action. For a scale-out
            # the gap from the action is container start-up + join, not
            # rebalancing, so both are reported.
            seen = sorted(set(step_records[step]["running_before"]) | live)
            callbacks = containers.storage_log_events(
                settings.log_dir, seen, {"assigned", "revoked", "lost"}, act["started_at"]
            )
            first_cb = parse_iso(callbacks[0]["ts"]) if callbacks else None
            unmoved_ok = {}
            for tp, a in activity.items():
                base = steady_activity[tp]
                is_moved = tp in moved_data
                activity_rows.append(
                    {"step": step, "topic": tp[0], "partition": tp[1], "moved": is_moved, **a}
                )
                if is_moved or a["max_delay_ms"] is None or base["max_delay_ms"] is None:
                    continue
                limit = max(2 * base["max_delay_ms"], base["max_delay_ms"] + 1000)
                unmoved_ok[f"{tp[0]}:{tp[1]}"] = {
                    "steady_max_delay_ms": base["max_delay_ms"],
                    "step_max_delay_ms": a["max_delay_ms"],
                    "limit_ms": limit,
                    "kept_processing": a["max_delay_ms"] <= limit,
                }
            owned_data = {
                inst: len(
                    [tp for tp, o in (after_owner or {}).items() if o == inst and tp in with_data]
                )
                for inst in live
            }
            res.update(
                {
                    "action": act["action"],
                    "action_target": act["target"],
                    "action_returned_at": act["returned_at"],
                    "rebalance_done_at": done["ts"] if done else None,
                    "rebalance_s": round((parse_iso(done["ts"]) - t_action).total_seconds(), 2)
                    if done
                    else None,
                    "first_callback_at": iso_ms(first_cb) if first_cb else None,
                    "action_to_first_callback_s": round((first_cb - t_action).total_seconds(), 2)
                    if first_cb
                    else None,
                    "first_callback_to_done_s": round(
                        (parse_iso(done["ts"]) - first_cb).total_seconds(), 2
                    )
                    if done and first_cb
                    else None,
                    "analysis_window_s": window_s,
                    "moved_partitions": [f"{t}:{p}" for t, p in moved],
                    "moved_data_partitions": [f"{t}:{p}" for t, p in moved_data],
                    "first_write_by_new_owner_s": first_writes,
                    "first_write_max_s": max(
                        [v for v in first_writes.values() if v is not None], default=None
                    ),
                    "moved_max_delay_ms": max(
                        [
                            activity[tp]["max_delay_ms"]
                            for tp in moved_data
                            if activity[tp]["max_delay_ms"] is not None
                        ],
                        default=None,
                    ),
                    "unmoved_data_partitions": unmoved_ok,
                    "unmoved_all_kept_processing": all(
                        v["kept_processing"] for v in unmoved_ok.values()
                    ),
                    "data_partitions_owned_per_instance": owned_data,
                    "every_live_instance_owns_data": bool(owned_data)
                    and all(n > 0 for n in owned_data.values()),
                    "owner_after": {
                        f"{t}:{p}": o for (t, p), o in sorted((after_owner or {}).items())
                    },
                }
            )
        step_results[step] = res

    with open(out_dir / "partition_activity.csv", "w", newline="") as f:
        fields = [
            "step",
            "topic",
            "partition",
            "moved",
            "rows",
            "max_delay_ms",
            "max_gap_ms",
            "instances",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in activity_rows:
            w.writerow({**r, "instances": "|".join(r["instances"])})
    tput = throughput(batches, steady_start)
    with open(out_dir / "throughput.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["t_s", "instance", "messages", "msgs_per_s"])
        w.writeheader()
        w.writerows(tput)

    rebalancing = [s for s in STEPS if s != "steady"]
    checks = {
        "no_events_lost": db["lost_total"] == 0 and db["expected_total"] > 0,
        "no_message_stored_twice": db["double_stored_total"] == 0,
        "seq_gaps_zero": db["seq_gaps"] == 0,
        "duplicates_zero_on_clean_steps": all(
            step_results[s]["duplicates"] == 0 for s in CLEAN_STEPS
        ),
        "rebalance_completed_every_step": all(
            step_results[s]["rebalance_s"] is not None for s in rebalancing
        ),
        "every_live_instance_owns_data": all(
            step_results[s]["every_live_instance_owns_data"] for s in rebalancing
        ),
        "unmoved_partitions_kept_processing": all(
            step_results[s]["unmoved_all_kept_processing"] for s in rebalancing
        ),
        "stopped_and_killed_not_auto_restarted": all(
            v["status"] == "exited" and v["restart_count"] == "0" for v in not_restarted.values()
        ),
    }
    summary = {
        "run_id": run_id,
        "experiment": "F",
        "simulated_feed": simulated,
        "producer_ws_host": urlparse(ws_url).hostname,
        "step_s": args.step,
        "analysis_window_s": ANALYSIS_WINDOW_S,
        "session_timeout_ms": 10_000,
        "assignment_strategy": "cooperative-sticky",
        "original_instances": [c["instance"] for c in original],
        "victims": {k: v["instance"] for k, v in victims.items()},
        "phases": events,
        "steps": step_results,
        "verification": db,
        "containers_after_run": not_restarted,
        "checks": checks,
        "all_passed": all(checks.values()),
    }
    write_json_atomic(out_dir / "summary.json", summary)
    (out_dir / "summary.md").write_text(render_markdown(summary))
    try:
        from experiments.make_charts import make_charts

        make_charts(out_dir)
    except Exception as exc:  # charts are helpful, never fatal
        say(f"chart generation failed: {exc}")

    if simulated and "_dev" not in out_dir.parts:
        dev_dir = settings.results_dir / "_dev" / run_id
        dev_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(out_dir), dev_dir)
        out_dir = dev_dir
    say(
        f"Done: {'ALL CHECKS PASSED' if summary['all_passed'] else 'SOME CHECKS FAILED'}"
        f"{'  (SIMULATED - not evidence)' if simulated else ''}"
    )
    say(f"Read {Path(out_dir).relative_to(REPO_ROOT)}/summary.md")
    say("Storage is now 1 instance; restore with: make scale N=2")
    return 0 if summary["all_passed"] else 1


def render_markdown(s: dict) -> str:
    """Human-readable summary. Every number comes from summary.json."""
    v = s["verification"]
    st = s["steps"]
    mark = lambda ok: "PASS" if ok else "FAIL"  # noqa: E731
    rb = [k for k in ("scale_out", "clean_stop", "crash")]
    lines = [
        f"# Experiment F (scale-out and rebalance) — {s['run_id']}",
        "",
        "**SIMULATED FEED — NOT EVIDENCE**"
        if s["simulated_feed"]
        else "Source: live TraderMade WebSocket V2 feed.",
        "",
        "All numbers were computed by `experiments/scale_rebalance.py` from the sampler CSVs, "
        "`actions.json` and TimescaleDB. None were typed by hand.",
        "",
        f"Storage group `storage-svc`, assignor {s['assignment_strategy']}, session.timeout.ms "
        f"{s['session_timeout_ms']}, {s['step_s']:.0f} s per step. Original instances "
        f"{', '.join(s['original_instances'])}; clean stop {s['victims']['clean_stop']}, "
        f"crash {s['victims']['crash']}.",
        "",
        "## Checks",
        "",
        "| Check | Result |",
        "| --- | --- |",
        *[f"| {k.replace('_', ' ')} | {mark(ok)} |" for k, ok in s["checks"].items()],
        "",
        "## Steps",
        "",
        "| Measure | " + " | ".join(rb) + " |",
        "| --- | " + " | ".join("---" for _ in rb) + " |",
        "| Action | "
        + " | ".join(f"{st[k]['action']} {st[k]['action_target']}" for k in rb)
        + " |",
        "| Action started (UTC) | " + " | ".join(st[k]["started_at"][11:23] for k in rb) + " |",
        "| Action -> rebalance done: STABLE, every partition owned by a live member (s) | "
        + " | ".join(str(st[k]["rebalance_s"]) for k in rb)
        + " |",
        "| of which: action -> first rebalance callback in any instance (s) | "
        + " | ".join(str(st[k]["action_to_first_callback_s"]) for k in rb)
        + " |",
        "| of which: first callback -> rebalance done (s) | "
        + " | ".join(str(st[k]["first_callback_to_done_s"]) for k in rb)
        + " |",
        "| Partitions moved (all / carrying data) | "
        + " | ".join(
            f"{len(st[k]['moved_partitions'])} / {len(st[k]['moved_data_partitions'])}" for k in rb
        )
        + " |",
        "| First write by new owner, slowest moved data partition (s) | "
        + " | ".join(str(st[k]["first_write_max_s"]) for k in rb)
        + " |",
        "| Max processing delay, moved data partitions (ms) | "
        + " | ".join(str(st[k]["moved_max_delay_ms"]) for k in rb)
        + " |",
        "| Unmoved data partitions kept processing | "
        + " | ".join(mark(st[k]["unmoved_all_kept_processing"]) for k in rb)
        + " |",
        "| Data partitions owned per live instance | "
        + " | ".join(
            ", ".join(
                f"{i}: {n}" for i, n in sorted(st[k]["data_partitions_owned_per_instance"].items())
            )
            for k in rb
        )
        + " |",
        "| Duplicates suppressed during the step | "
        + " | ".join(str(st[k]["duplicates"]) for k in rb)
        + " |",
        "| Messages written during the step | "
        + " | ".join(str(st[k]["messages_written"]) for k in rb)
        + " |",
        "",
        f"Steady step: {st['steady']['messages_written']} messages written by "
        f"{', '.join(st['steady']['instances_writing'])}, duplicates {st['steady']['duplicates']}.",
        "",
        "## Unmoved partitions during each rebalance (processing delay = stored_at - Kafka timestamp)",
        "",
        "| Step | Topic-partition | Steady max (ms) | Step max (ms) | Limit (ms) | Kept processing |",
        "| --- | --- | --- | --- | --- | --- |",
        *[
            f"| {k} | {tp} | {d['steady_max_delay_ms']} | {d['step_max_delay_ms']} | {d['limit_ms']} | {mark(d['kept_processing'])} |"
            for k in rb
            for tp, d in st[k]["unmoved_data_partitions"].items()
        ],
        "",
        "## No-loss verification per topic (Kafka offsets vs TimescaleDB rows)",
        "",
        "| Topic | Expected | Stored | Lost | Seq gaps |",
        "| --- | --- | --- | --- | --- |",
        *[
            f"| {short(t)} | {d['expected']} | {d['stored']} | {d['lost']} | {d.get('seq_gaps', 0)} |"
            for t, d in v["per_topic"].items()
        ],
        "",
        f"Total expected {v['expected_total']}, stored {v['stored_total']}, lost {v['lost_total']}, "
        f"stored twice {v['double_stored_total']}, seq gaps {v['seq_gaps']}.",
        "",
        "Charts: `ownership.png` (who owned each topic-partition), `lag_by_topic.png`, `throughput.png`.",
        "Raw: `actions.json`, `ownership.csv`, `group_states.csv`, `partition_activity.csv`, "
        "`describe_*_before/after.txt`.",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
