"""
Failure-and-recovery experiments A, B and C on the v2 stack (containers,
three topics, TimescaleDB, N Storage instances).

One command runs a whole scenario against the LIVE pipeline and writes every
number the article will quote into results/<run-id>/, computed from data, so
nothing is ever typed by hand.

    make experiment-a    --mode stop   ALL Storage instances stopped cleanly (docker stop)
    make experiment-b    --mode crash  ALL Storage instances killed (docker kill -s KILL)
    make experiment-c    --mode slow   Storage replaced by ONE slow instance, then restored

Before you run it
    * FX market open (checked via REST unless --skip-market-check); for the
      evidence day, US market hours so all three topics tick
    * the stack running (`make up`), N >= 1 Storage instances (`make scale N=2`)

Timeline (durations are flags)
    setup     wait until Storage's group has N members and is caught up
    baseline  --baseline s (default 120): everything normal; record lag
    outage    --outage s (default 300): stop / kill / slow ALL instances
    catchup   `docker start` the SAME containers (same hostnames, so the same
              instance_ids and log files); wait until lag is back to baseline
    settle    --settle s (default 120): everything normal again
    verify    check TimescaleDB against Kafka, per topic-partition
Storage is left RUNNING at the end: in v2 the stack keeps going.

What is measured, and where it comes from
    lag.csv, group_states.csv, ownership.csv   experiments/sampler.py
    storage_batches.csv  every Storage write: instance, size, duplicates, age
    actions.json         every docker action with start and return time
    admin_*.json         committed offsets at outage start and before restart
    describe_*.txt       kafka-consumer-groups --describe (+ --members --verbose)
    events.json          phase start times
    summary.json/.md     results with a per-topic breakdown and PASS/FAIL checks
    lag.png, isolation.png, catchup.png, lag_by_topic.png   (make_charts.py)

Simulated feeds are never evidence: if the producer container points at the
simulator, or any verified row says source=simulated, the run is written
under results/_dev/ and labelled SIMULATED.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from experiments import containers, db_checks
from experiments.sampler import Sampler
from pipeline import rest
from pipeline.common import (
    GROUP_MONITOR,
    GROUP_PRICING,
    GROUP_RISK,
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
from pipeline.producer import OFFICIAL_WS_HOST

SLOW_CONTAINER = "tm-storage-slow"
OTHER_GROUPS = (GROUP_PRICING, GROUP_RISK, GROUP_MONITOR)


def say(msg: str) -> None:
    """Progress line with a wall-clock time, so a screen recording is readable."""
    print(f"{datetime.now():%H:%M:%S}  {msg}", flush=True)


def wait_until(predicate, timeout: float, what: str) -> float:
    """Poll `predicate` once a second; return seconds waited or raise."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if predicate():
            return time.monotonic() - t0
        time.sleep(1.0)
    raise TimeoutError(f"timed out after {timeout:.0f}s waiting for: {what}")


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def tp_key(tp) -> str:
    return f"{tp[0]}:{tp[1]}"


def short(topic: str) -> str:
    """marketdata.fx.quotes.v1 -> fx, for table labels."""
    return topic.replace("marketdata.", "").replace(".quotes.v1", "")


def committed_snapshot(admin, probe, tps) -> dict:
    s = sample_groups(admin, probe, [GROUP_STORAGE], tps)[0]
    return {
        "state": s.state,
        "members": s.members,
        "committed": {tp_key(tp): c for tp, c in s.committed.items()},
        "end": {tp_key(tp): e for tp, e in s.end.items()},
        "lag": s.lag,
        "lag_by_topic": {t: s.for_topic(t).lag for t in sorted({tp[0] for tp in tps})},
    }


def catchup_by_event_age(batches, restart_iso: str, fresh_ms: float):
    """From Storage's own write log: seconds from restart until the first
    batch whose OLDEST message was younger than `fresh_ms` (i.e. Storage is
    processing live data again), plus how many messages it wrote meanwhile."""
    t_restart = parse_iso(restart_iso)
    written = 0
    for b in batches:  # dicts from db_checks.batches_since, oldest first
        t = b["written_at"]
        if t < t_restart:
            continue
        written += b["messages"]
        if b["max_age_ms"] <= fresh_ms:
            return round((t - t_restart).total_seconds(), 2), written
    return None, written


def main() -> int:
    ap = argparse.ArgumentParser(description="Failure and recovery experiments A/B/C (v2)")
    ap.add_argument("--mode", choices=("stop", "crash", "slow"), required=True)
    ap.add_argument("--baseline", type=float, default=120, help="seconds of normal running first")
    ap.add_argument("--outage", type=float, default=300, help="seconds Storage is down/slow")
    ap.add_argument(
        "--settle", type=float, default=120, help="seconds of normal running at the end"
    )
    ap.add_argument("--catchup-timeout", type=float, default=900)
    ap.add_argument(
        "--slow-delay-ms",
        type=int,
        default=0,
        help="slow mode delay per message; 0 = choose so Storage runs at half the ingress rate",
    )
    ap.add_argument(
        "--allowance",
        type=int,
        default=20,
        help="isolation check: extra lag (messages) tolerated over baseline for other groups",
    )
    ap.add_argument(
        "--behind-s",
        type=float,
        default=0,
        help="setup: caught up = committed everything older than this; 0 = commit interval + 5 s",
    )
    ap.add_argument("--skip-market-check", action="store_true", help="DEV ONLY")
    args = ap.parse_args()

    settings = Settings.from_env()
    ws_url = containers.producer_ws_url()
    simulated_url = urlparse(ws_url).hostname != OFFICIAL_WS_HOST
    letter = {"stop": "A", "crash": "B", "slow": "C"}[args.mode]
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-exp{letter}-{args.mode}"
    out_dir = settings.results_dir / ("_dev" if simulated_url else "") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    say(f"Experiment {letter} ({args.mode}) -> {out_dir.relative_to(REPO_ROOT)}")

    # ---- preflight ---------------------------------------------------------
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
    instances = containers.storage_instances()
    if not instances:
        say("No Storage instance is running: make scale N=2")
        return 2
    n = len(instances)
    names = [c["name"] for c in instances]
    ids = [c["instance"] for c in instances]
    say(f"Storage instances: {n} ({', '.join(ids)})")

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    probe = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap, "group.id": "experiment-probe"}
    )
    topics = settings.topology.topic_names
    tps = topic_partitions(admin, topics)
    t_mono0 = time.monotonic()
    actions = containers.Actions()
    events: dict[str, str] = {}
    sampler = Sampler(settings, tps, out_dir, t_mono0, interval=1.0)
    sampler.start()

    def phase(name: str) -> None:
        sampler.phase = name
        events[name] = iso_ms(utc_now())
        say(f"--- phase: {name}")

    slow_delay = 0
    try:
        # ---- setup: Storage group complete and caught up --------------------
        phase("setup")
        waited = wait_until(
            lambda: (
                (r := sampler.latest(GROUP_STORAGE)) is not None
                and r["members"] == n
                and sampler.caught_up(
                    args.behind_s or settings.storage_offset_commit_interval_s + 5
                )
            ),
            timeout=args.catchup_timeout,
            what=f"Storage to have {n} members and be caught up",
        )
        say(f"Storage caught up after {waited:.0f}s")
        run_start_iso = iso_ms(utc_now())
        start_offsets = end_offsets(probe, tps)

        # ---- baseline ------------------------------------------------------
        phase("baseline")
        time.sleep(args.baseline)
        end_at_baseline = end_offsets(probe, tps)
        ingress_rate = (sum(end_at_baseline.values()) - sum(start_offsets.values())) / args.baseline
        ingress_by_topic = {
            t: round(
                sum(end_at_baseline[tp] - start_offsets[tp] for tp in tps if tp[0] == t)
                / args.baseline,
                3,
            )
            for t in topics
        }
        say(f"Ingress during baseline: {ingress_rate:.2f} msg/s {ingress_by_topic}")

        # ---- outage: ALL instances at once, one docker command -------------
        containers.capture_describe(out_dir / "describe_before_outage.txt", "before outage")
        phase("outage")
        if args.mode == "stop":
            actions.run("stop", "all storage", ["docker", "stop", "-t", "30", *names])
        elif args.mode == "crash":
            actions.run("kill", "all storage", ["docker", "kill", "-s", "KILL", *names])
        else:
            # Slow: ONE instance with a per-message delay that makes it process
            # at half the measured ingress rate, so lag must grow.
            slow_delay = args.slow_delay_ms or max(1, int(1000 / max(ingress_rate * 0.5, 0.1)))
            actions.run("stop", "all storage", ["docker", "stop", "-t", "30", *names])
            actions.run(
                "run_slow",
                SLOW_CONTAINER,
                [
                    "docker",
                    "compose",
                    "run",
                    "-d",
                    "--no-deps",  # the stack is already up: do not re-run init jobs
                    "--name",
                    SLOW_CONTAINER,
                    "-e",
                    f"STORAGE_DELAY_MS={slow_delay}",
                    "storage",
                ],
            )
        outage_started = time.monotonic()
        write_json_atomic(
            out_dir / "admin_at_outage_start.json", committed_snapshot(admin, probe, tps)
        )
        time.sleep(max(0.0, args.outage - (time.monotonic() - outage_started)))

        # Were the stopped/killed containers left down? (no restart policy)
        not_restarted = {name: containers.container_state(name) for name in names}

        # ---- restart / restore ----------------------------------------------
        if args.mode == "slow":
            actions.run("stop", SLOW_CONTAINER, ["docker", "stop", "-t", "30", SLOW_CONTAINER])
            actions.run("rm", SLOW_CONTAINER, ["docker", "rm", SLOW_CONTAINER])
        containers.capture_describe(out_dir / "describe_before_restart.txt", "before restart")
        before_restart = committed_snapshot(admin, probe, tps)
        write_json_atomic(out_dir / "admin_before_restart.json", before_restart)
        phase("catchup")
        restart_iso = events["catchup"]
        actions.run("start", "all storage", ["docker", "start", *names])
        storage_baseline_max = sampler.max_lag(GROUP_STORAGE, "baseline") or 0
        threshold = max(storage_baseline_max, args.allowance)
        catchup_s = wait_until(
            lambda: (
                (r := sampler.latest(GROUP_STORAGE)) is not None
                and r["phase"] == "catchup"
                and r["members"] == n
                and r["lag"] is not None
                and r["lag"] <= threshold
            ),
            timeout=args.catchup_timeout,
            what="Storage lag back to its baseline level",
        )
        say(f"Storage lag back to baseline (<= {threshold}) after {catchup_s:.1f}s")

        # ---- settle --------------------------------------------------------
        phase("settle")
        time.sleep(args.settle)
        end_final = end_offsets(probe, tps)

        # Let Storage commit past the final offsets. A partition that never
        # held a message (end offset 0) has nothing to commit: with key =
        # symbol, 10 of the 18 partitions stay empty.
        def committed_past_range() -> bool:
            s = sample_groups(admin, probe, [GROUP_STORAGE], tps)[0]
            return all(o == 0 or s.committed.get(tp, -1) >= o for tp, o in end_final.items())

        wait_until(
            committed_past_range, timeout=120, what="Storage to commit past the verification range"
        )
        phase("verify")
    finally:
        sampler.stopped.set()
        sampler.join(timeout=10)
        probe.close()
        actions.save(out_dir / "actions.json")
        write_json_atomic(out_dir / "events.json", events)

    # ---- verification ---------------------------------------------------------
    with db_checks.connect(settings) as conn:
        db = db_checks.verify_range(conn, start_offsets, end_final)
        batches = db_checks.batches_since(conn, parse_iso(run_start_iso))
    db_checks.write_batches_csv(batches, out_dir / "storage_batches.csv")
    db["duplicates_suppressed"] = sum(b["duplicates"] for b in batches)
    simulated = simulated_url or "simulated" in db["sources"]

    # Did the restarted instances resume exactly at the committed offsets?
    # Per topic-partition, the FIRST "resumed" after restart (from any
    # instance's log) must equal the offset committed before restart.
    resumed = containers.storage_log_events(settings.log_dir, ids, {"resumed"}, restart_iso)
    first_read: dict[str, int] = {}
    for r in resumed:
        first_read.setdefault(f"{r['topic']}:{r['partition']}", r["offset"])
    resume_rows = [
        {
            "topic_partition": k,
            "committed_before_restart": before_restart["committed"].get(k),
            "first_offset_read": first_read.get(k),
        }
        for k in sorted(before_restart["committed"])
        if first_read.get(k) is not None or before_restart["committed"].get(k, -1) >= 0
    ]
    resume_ok = bool(first_read) and all(
        r["first_offset_read"] is None or r["first_offset_read"] == r["committed_before_restart"]
        for r in resume_rows
    )

    fresh_ms = 2000.0
    age_catchup_s, age_catchup_msgs = catchup_by_event_age(batches, restart_iso, fresh_ms)
    isolation = {}
    for g in OTHER_GROUPS:
        per_topic = {}
        for t in ["*", *topics]:
            base = sampler.max_lag(g, "baseline", t) or 0
            out = sampler.max_lag(g, "outage", t) or 0
            per_topic[t] = {
                "baseline_max_lag": base,
                "outage_max_lag": out,
                "ok": out <= max(2 * base, base + args.allowance),
            }
        isolation[g] = per_topic
    storage_lag_at_restart = before_restart["lag"]

    checks = {
        "other_groups_unaffected": all(d["ok"] for per in isolation.values() for d in per.values()),
        "no_events_lost": db["lost_total"] == 0 and db["expected_total"] > 0,
        "no_message_stored_twice": db["double_stored_total"] == 0,
        "seq_gaps_zero": db["seq_gaps"] == 0,
        "resumed_at_committed_offset": resume_ok,
        "storage_lag_grew_during_outage": (storage_lag_at_restart or 0) > threshold,
        "stopped_instances_not_auto_restarted": all(
            s["status"] == "exited" and s["restart_count"] == "0" for s in not_restarted.values()
        ),
    }
    if args.mode == "crash":
        checks["redelivery_observed_after_crash"] = db["duplicates_suppressed"] > 0
    summary = {
        "run_id": run_id,
        "mode": args.mode,
        "experiment": letter,
        "simulated_feed": simulated,
        "producer_ws_host": urlparse(ws_url).hostname,
        "storage_instances": ids,
        "phases": events,
        "durations_s": {"baseline": args.baseline, "outage": args.outage, "settle": args.settle},
        "ingress_msgs_per_s_baseline": round(ingress_rate, 3),
        "ingress_msgs_per_s_baseline_by_topic": ingress_by_topic,
        "slow_delay_ms_per_msg": slow_delay or None,
        "storage_lag_at_restart": storage_lag_at_restart,
        "storage_lag_at_restart_by_topic": before_restart["lag_by_topic"],
        "storage_baseline_max_lag": storage_baseline_max,
        "catchup_s_by_committed_lag": round(catchup_s, 2),
        "catchup_s_by_event_age": age_catchup_s,
        "catchup_event_age_threshold_ms": fresh_ms,
        "catchup_msgs_written": age_catchup_msgs,
        "catchup_rate_msgs_per_s": (
            round(age_catchup_msgs / age_catchup_s, 1) if age_catchup_s else None
        ),
        "isolation": isolation,
        "verification": db,
        "resume": resume_rows,
        "containers_after_outage": not_restarted,
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
    return 0 if summary["all_passed"] else 1


def render_markdown(s: dict) -> str:
    """Human-readable summary. Every number comes from summary.json."""
    v = s["verification"]
    mark = lambda ok: "PASS" if ok else "FAIL"  # noqa: E731
    lines = [
        f"# Experiment {s['experiment']} ({s['mode']}) — {s['run_id']}",
        "",
        "**SIMULATED FEED — NOT EVIDENCE**"
        if s["simulated_feed"]
        else "Source: live TraderMade WebSocket V2 feed.",
        "",
        "All numbers below were computed by `experiments/failure_recovery.py` from `lag.csv` "
        "and TimescaleDB. None were typed by hand.",
        "",
        "## Checks",
        "",
        "| Check | Result |",
        "| --- | --- |",
        *[f"| {k.replace('_', ' ')} | {mark(ok)} |" for k, ok in s["checks"].items()],
        "",
        "## Run",
        "",
        "| Item | Value |",
        "| --- | --- |",
        f"| Storage instances | {len(s['storage_instances'])} ({', '.join(s['storage_instances'])}) |",
        *[f"| Phase `{k}` started | {t} |" for k, t in s["phases"].items()],
        f"| Baseline / outage / settle (s) | {s['durations_s']['baseline']:.0f} / {s['durations_s']['outage']:.0f} / {s['durations_s']['settle']:.0f} |",
        f"| Ingress during baseline (msg/s) | {s['ingress_msgs_per_s_baseline']} |",
        *[
            f"| Ingress, {short(t)} (msg/s) | {r} |"
            for t, r in s["ingress_msgs_per_s_baseline_by_topic"].items()
        ],
        f"| Slow-mode delay (ms/msg) | {s['slow_delay_ms_per_msg'] or '-'} |",
        "",
        "## Storage outage and recovery",
        "",
        "| Measure | Value |",
        "| --- | --- |",
        f"| Storage max lag during baseline (messages) | {s['storage_baseline_max_lag']} |",
        f"| Storage lag at restart (messages) | {s['storage_lag_at_restart']} |",
        *[
            f"| Storage lag at restart, {short(t)} | {lag} |"
            for t, lag in s["storage_lag_at_restart_by_topic"].items()
        ],
        f"| Catch-up time, committed lag back to baseline (s) | {s['catchup_s_by_committed_lag']} |",
        f"| Catch-up time, first batch with event age ≤ {s['catchup_event_age_threshold_ms']:.0f} ms (s) | {s['catchup_s_by_event_age']} |",
        f"| Messages written during catch-up | {s['catchup_msgs_written']} |",
        f"| Catch-up processing rate (msg/s) | {s['catchup_rate_msgs_per_s']} |",
        "",
        "## Other consumers during the outage (max lag, messages)",
        "",
        "| Group | Topic | Baseline | Outage | Isolation |",
        "| --- | --- | --- | --- | --- |",
        *[
            f"| {g} | {'all' if t == '*' else short(t)} | {d['baseline_max_lag']} | {d['outage_max_lag']} | {mark(d['ok'])} |"
            for g, per in s["isolation"].items()
            for t, d in per.items()
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
        f"stored twice {v['double_stored_total']}, seq gaps {v['seq_gaps']}, "
        f"duplicates suppressed {v['duplicates_suppressed']}.",
        "",
        "| Topic-partition | From offset | To offset | Expected | Stored |",
        "| --- | --- | --- | --- | --- |",
        *[
            f"| {short(k.rsplit(':', 1)[0])}:{k.rsplit(':', 1)[1]} | {d['from_offset']} | {d['to_offset']} | {d['expected']} | {d['stored']} |"
            for k, d in v["per_partition"].items()
            if d["expected"] or d["stored"]
        ],
        "",
        "## Resume point (first offset read after restart vs committed before)",
        "",
        "| Topic-partition | Committed before restart | First offset read |",
        "| --- | --- | --- |",
        *[
            f"| {r['topic_partition']} | {r['committed_before_restart']} | {r['first_offset_read']} |"
            for r in s["resume"]
        ],
        "",
        "Charts: `lag.png` (all groups), `isolation.png`, `lag_by_topic.png`, `catchup.png`.",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
