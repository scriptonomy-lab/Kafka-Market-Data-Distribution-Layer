"""
Preflight: one command that tells you whether the v2 stack is ready to record
evidence. Every check prints PASS / WARN / FAIL / INFO with a reason; secrets
are only ever reported as "set" or "missing".

Checks
    1. TraderMade keys are set (.env, read by host tools and handed to the
       producer / seed-book containers as Compose secrets)
    2. Compose services: kafka and timescaledb healthy, kafka-init and
       db-init exited 0, producer / pricing / risk / monitor running, and how
       many Storage instances run
    3. The three topics exist with the partition counts in topology.toml
    4. TimescaleDB reachable, `ticks` is a hypertable, migrations applied
    5. Symbol count within the plan's symbol_limit
    6. Markets: FX status via TraderMade REST (/market_open_status), every
       other market REST reports, and a clock check for US equity hours
       (US stocks only tick 09:30-16:00 New York time on trading days)
    7. The producer's observed partition map matches the computed one
    8. Every consumer group: status, members, lag (same sampler as the monitor)

Exit code 0 when nothing FAILed, 1 otherwise, so scripts can gate on it.

Run:  make preflight
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from pipeline import rest
from pipeline.common import (
    ALL_GROUPS,
    REPO_ROOT,
    Settings,
    db_conninfo,
    derive_status,
    sample_groups,
    topic_partitions,
)
from pipeline.topology import expected_partition_map

RUNNING_SERVICES = ("producer", "pricing", "risk", "monitor")
ONE_SHOT = ("kafka-init", "db-init")
NEW_YORK = ZoneInfo("America/New_York")


def report(status: str, check: str, detail: str = "") -> None:
    print(f"[{status:<4}] {check:<28} {detail}")


def compose_states() -> dict[str, list[dict]]:
    """service -> its containers' states, from `docker compose ps -a --format json`."""
    out = subprocess.run(
        ["docker", "compose", "ps", "-a", "--format", "json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    states: dict[str, list[dict]] = {}
    for line in out.stdout.splitlines():
        if line.strip():
            c = json.loads(line)
            states.setdefault(c["Service"], []).append(c)
    return states


def us_equity_hours(now: datetime) -> bool:
    """09:30-16:00 New York, Monday-Friday. A clock check only: exchange
    holidays and half days are NOT known here."""
    ny = now.astimezone(NEW_YORK)
    minutes = ny.hour * 60 + ny.minute
    return ny.weekday() < 5 and 9 * 60 + 30 <= minutes < 16 * 60


def main() -> int:
    settings = Settings.from_env()
    failed = False

    def fail(check: str, detail: str) -> None:
        nonlocal failed
        report("FAIL", check, detail)
        failed = True

    # 1. Keys (presence only — never print them)
    for name, value in (
        ("TRADERMADE_WS_KEY", settings.ws_key),
        ("TRADERMADE_REST_KEY", settings.rest_key),
    ):
        report("PASS", name, "set") if value else fail(name, "missing in .env")

    # 2. Containers
    try:
        states = compose_states()
    except (OSError, subprocess.TimeoutExpired) as exc:
        states = {}
        fail("Docker", f"docker compose ps failed: {exc}")
    for svc in ("kafka", "timescaledb"):
        c = (states.get(svc) or [{}])[0]
        ok = c.get("State") == "running" and c.get("Health") == "healthy"
        detail = f"{c.get('State', 'missing')} / {c.get('Health') or '-'}"
        report("PASS", f"service {svc}", detail) if ok else fail(f"service {svc}", detail)
    for svc in ONE_SHOT:
        c = (states.get(svc) or [{}])[0]
        ok = c.get("State") == "exited" and c.get("ExitCode") == 0
        detail = f"{c.get('State', 'missing')} (exit {c.get('ExitCode', '-')})"
        report("PASS", f"job {svc}", detail) if ok else fail(f"job {svc}", detail)
    for svc in RUNNING_SERVICES:
        c = (states.get(svc) or [{}])[0]
        state = c.get("State", "missing")
        report("PASS" if state == "running" else "WARN", f"service {svc}", state)
    storage = [c for c in states.get("storage", []) if c.get("State") == "running"]
    report(
        "PASS" if storage else "WARN",
        "service storage",
        f"{len(storage)} instance(s) running: {', '.join(c['Name'] for c in storage) or '-'}",
    )

    # 3. Topics, with the partition counts topology.toml asks for
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    try:
        admin.list_topics(timeout=5)
    except Exception as exc:
        fail("Kafka", f"not reachable at {settings.kafka_bootstrap}: {exc} (run: make up)")
        return 1
    tps = []
    for t in settings.topology.topics.values():
        try:
            found = topic_partitions(admin, [t.name])
        except KeyError:
            fail(f"topic {t.asset_class}", f"{t.name} missing (run: make create-topics)")
            continue
        detail = f"{t.name} ({len(found)} partitions, expected {t.partitions})"
        if len(found) == t.partitions:
            report("PASS", f"topic {t.asset_class}", detail)
        else:
            fail(f"topic {t.asset_class}", detail)
        tps.extend(found)

    # 4. Database
    try:
        import psycopg

        with psycopg.connect(db_conninfo(settings)) as conn:
            hyper = conn.execute(
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'ticks'"
            ).fetchone()[0]
            versions = [
                r[0] for r in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")
            ]
            rows = conn.execute("SELECT count(*) FROM ticks").fetchone()[0]
        detail = f"{settings.db_host}:{settings.db_port} migrations {versions}, ticks rows {rows}"
        report("PASS", "TimescaleDB", detail) if hyper == 1 else fail(
            "TimescaleDB", "ticks is not a hypertable"
        )
    except Exception as exc:
        fail(
            "TimescaleDB",
            f"{type(exc).__name__} at {settings.db_host}:{settings.db_port} (run: make up)",
        )

    # 5. Symbol count vs plan (topology.toml already refuses more than the
    # configured limit; the producer re-checks the live login_ok value)
    n = len(settings.topology.symbols)
    report(
        "PASS",
        "symbols vs plan",
        f"{n} symbols, symbol_limit {settings.topology.symbol_limit} (producer re-checks login_ok)",
    )

    # 6. Markets
    if settings.rest_key:
        try:
            statuses = rest.market_statuses(settings)
            fx = statuses.get("FX", {})
            detail = f"{fx.get('status')} (next close {fx.get('next_close') or '-'} GMT, next open {fx.get('next_open') or '-'} GMT)"
            report("PASS" if fx.get("status") == "open" else "WARN", "FX market (REST)", detail)
            # REST names a "SingleStocks" market; whether it is US-only is not
            # documented, so it is shown as REST names it, next to the clock check.
            stocks = statuses.get("SingleStocks", {}).get("status", "no entry")
            report(
                "INFO", "SingleStocks market (REST)", f"{stocks} (crypto has no REST entry: 24/7)"
            )
            others = {
                k: v.get("status") for k, v in statuses.items() if k not in ("FX", "SingleStocks")
            }
            report(
                "INFO", "other markets (REST)", json.dumps(others) if others else "none reported"
            )
        except rest.RestError as exc:
            report("WARN", "market status (REST)", str(exc))
    now = datetime.now(NEW_YORK)
    in_hours = us_equity_hours(now)
    report(
        "PASS" if in_hours else "WARN",
        "US equity hours (clock)",
        f"{now:%a %H:%M} New York: {'inside' if in_hours else 'OUTSIDE'} 09:30-16:00 Mon-Fri "
        "(holidays not checked). US stocks only tick in market hours; record evidence "
        "09:45-11:30 ET so all three asset classes tick.",
    )

    # 7. Partition map: computed vs what the producer observed
    observed_path = settings.run_dir / "partition_map.json"
    if observed_path.exists():
        observed = json.loads(observed_path.read_text())
        expected = expected_partition_map(settings.topology)
        diffs = [
            f"{t} {s}: expected {p}, observed {observed.get(t, {}).get(s)}"
            for t, syms in expected.items()
            for s, p in syms.items()
            if observed.get(t, {}).get(s) not in (None, p)
        ]
        missing = sum(
            observed.get(t, {}).get(s) is None for t, syms in expected.items() for s in syms
        )
        if diffs:
            fail("partition map", "; ".join(diffs))
        else:
            report(
                "PASS",
                "partition map",
                f"observed matches computed ({missing} symbol(s) not yet seen)",
            )
    else:
        report("INFO", "partition map", "run/partition_map.json not written yet (producer not run)")

    # 8. Consumer groups. This probe consumer never subscribes, so it never
    # joins or creates a group; it is only used to ask for end offsets.
    probe = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": "preflight-probe"})
    try:
        for s in sample_groups(admin, probe, ALL_GROUPS, tps):
            status = derive_status(s, [], time.monotonic())
            lag = "-" if s.lag is None else s.lag
            report(
                "INFO",
                f"group {s.group}",
                f"{status} state={s.state} members={s.members} lag={lag}",
            )
    finally:
        probe.close()

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
