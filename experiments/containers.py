"""
Container control for the experiments, through the docker CLI only.

v1's runner started Storage as a child process and signalled its PID. In v2
every service is a container, so the experiments act on containers exactly
as an operator would, and record WHEN each action started and returned:

    clean stop   docker stop <container>          SIGTERM, wait, exit 0
    crash        docker kill -s KILL <container>  no shutdown code runs at all
    restart      docker start <container>         same container, same hostname
                                                  = same instance_id
    scale        docker compose up -d --scale storage=N --no-recreate storage
                                                  adds instances, leaves the
                                                  running ones untouched

Every call goes through Actions.run(), so actions.json in the run folder is
the exact timeline (UTC, milliseconds) that rebalance durations are measured
from. Nothing here needs a secret or reads .env.
"""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.common import GROUP_STORAGE, REPO_ROOT, iso_ms, utc_now

KAFKA_GROUPS_CLI = [
    "docker",
    "compose",
    "exec",
    "-T",
    "kafka",
    "/opt/kafka/bin/kafka-consumer-groups.sh",
]


def _run(cmd: list[str], timeout: float = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)


def compose_ps(service: str = "") -> list[dict]:
    """`docker compose ps -a --format json [service]`, one dict per container."""
    out = _run(
        ["docker", "compose", "ps", "-a", "--format", "json", *([service] if service else [])]
    )
    return [json.loads(line) for line in out.stdout.splitlines() if line.strip()]


def storage_instances(running_only: bool = True) -> list[dict]:
    """Storage containers as {"name", "instance", "state"}. instance = the
    container's hostname = its 12-character id = Storage's instance_id."""
    out = []
    for c in compose_ps("storage"):
        if running_only and c.get("State") != "running":
            continue
        out.append({"name": c["Name"], "instance": c["ID"][:12], "state": c.get("State")})
    return sorted(out, key=lambda c: c["name"])


def container_state(name: str) -> dict:
    """State, exit code and restart count of one container (for the
    "not auto-restarted" check)."""
    out = _run(
        ["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}", name]
    )
    status, code, restarts = (out.stdout.split() + ["?", "?", "?"])[:3]
    return {"status": status, "exit_code": code, "restart_count": restarts}


def running(service: str) -> bool:
    return any(c.get("State") == "running" for c in compose_ps(service))


@dataclass
class Actions:
    """Runs docker commands and keeps the timeline of what was done when."""

    log: list[dict] = field(default_factory=list)

    def run(self, action: str, target: str, cmd: list[str], timeout: float = 120) -> dict:
        started = utc_now()
        t0 = time.monotonic()
        out = _run(cmd, timeout=timeout)
        rec = {
            "action": action,
            "target": target,
            "command": " ".join(cmd),
            "started_at": iso_ms(started),
            "returned_at": iso_ms(utc_now()),
            "duration_s": round(time.monotonic() - t0, 3),
            "exit_code": out.returncode,
        }
        if out.returncode != 0:
            rec["stderr"] = out.stderr.strip()[-500:]
        self.log.append(rec)
        print(
            f"{rec['started_at'][11:23]}  {action:<10} {target}  ({rec['duration_s']} s, exit {out.returncode})",
            flush=True,
        )
        if out.returncode != 0:
            raise RuntimeError(f"{action} {target} failed: {rec.get('stderr')}")
        return rec

    def stop(self, name: str) -> dict:
        # -t 30 matches stop_grace_period in docker-compose.yml
        return self.run("stop", name, ["docker", "stop", "-t", "30", name])

    def kill(self, name: str) -> dict:
        return self.run("kill", name, ["docker", "kill", "-s", "KILL", name])

    def start(self, name: str) -> dict:
        return self.run("start", name, ["docker", "start", name])

    def scale(self, n: int) -> dict:
        return self.run(
            "scale",
            f"storage={n}",
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--scale",
                f"storage={n}",
                "--no-recreate",
                "storage",
            ],
            timeout=300,
        )

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(self.log, indent=2))


def capture_describe(path: Path, note: str = "") -> None:
    """Kafka's own CLI view of the storage group: offsets/lag per
    topic-partition, then members with their assignments (--members --verbose)."""
    parts = [f"# captured {iso_ms(utc_now())} {note}".rstrip()]
    for extra in ([], ["--members", "--verbose"]):
        cmd = [
            *KAFKA_GROUPS_CLI,
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            GROUP_STORAGE,
            *extra,
        ]
        try:
            out = _run(cmd, timeout=60)
            parts.append(
                f"$ kafka-consumer-groups.sh --describe --group {GROUP_STORAGE} {' '.join(extra)}\n{out.stdout}{out.stderr}"
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            parts.append(f"CLI capture unavailable: {exc}")
    path.write_text("\n".join(parts))


def producer_ws_url() -> str:
    """The WebSocket URL the producer CONTAINER uses (with `make up-dev` it is
    the simulator, whatever .env says). Decides whether a run is evidence."""
    out = _run(
        ["docker", "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", "tm-producer"]
    )
    for line in out.stdout.splitlines():
        if line.startswith("TRADERMADE_WS_URL="):
            return line.split("=", 1)[1]
    return ""


def storage_log_events(
    log_dir: Path, instances: list[str], events: set[str], since_iso: str
) -> list[dict]:
    """Structured events from logs/storage-<instance>.log at or after
    `since_iso` (ISO strings in one format compare correctly as text)."""
    out = []
    for inst in instances:
        path = log_dir / f"storage-{inst}.log"
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("event") in events and rec.get("ts", "") >= since_iso:
                out.append(rec)
    return sorted(out, key=lambda r: r["ts"])
