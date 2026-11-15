"""
Build dist/evidence-<date>.zip: everything a reviewer needs to check the v2
results, and nothing that must not leave this machine.

Included (an ALLOWLIST: anything not listed here is left out)
    README.md, docs/results-v1-sqlite.md, docs/RUNBOOK_V2.md
    docs/evidence/**                 screenshots
    results/<run>/  for every LIVE v2 run: summary.json says
                    "simulated_feed": false and the run has the v2 per-topic
                    breakdown. Never results/_dev/, never v1 folders.
    results/<stamp>-tests/           make test-report output
    results/<stamp>-assignment-check/  make assignment-check ARGS=--save
    logs/producer.log                connection, sub_ack, routing, metrics;
                                     the masked key in login_ok is replaced by
                                     "[redacted]" in the bundled copy
    logs/storage-*.log               assigned / revoked / lost / resumed, stats
    logs/monitor.log                 rebalance_observed

Never included
    data/ (raw ticks, captures), database dumps, .env, secrets/, and the
    price-bearing logs/pricing.log and logs/risk.log. TraderMade data is
    licensed; result folders hold counts, offsets, lags and timings only.

Safety net: after building, every file in the zip is scanned for the
TraderMade keys from the environment; if either appears, the zip is deleted
and the script fails. Keys are compared, never printed.

Run:  make evidence-bundle
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from pipeline.common import REPO_ROOT, Settings

DOCS = ["README.md", "docs/results-v1-sqlite.md", "docs/RUNBOOK_V2.md"]
LOG_PATTERNS = ["producer.log", "storage-*.log", "monitor.log"]
MASKED_KEY = re.compile(r'("key":\s*")[^"]*(")')


def is_live_v2_run(folder: Path) -> bool:
    """A results folder from a LIVE v2 experiment run."""
    summary = folder / "summary.json"
    if not summary.exists():
        return False
    try:
        s = json.loads(summary.read_text())
    except json.JSONDecodeError:
        return False
    v2 = "per_topic" in s.get("verification", {}) or "comparison_per_topic" in s
    return s.get("simulated_feed") is False and v2


def collect() -> list[tuple[Path, str]]:
    """(source file, path inside the zip) for everything allowed."""
    files: list[tuple[Path, str]] = []
    for rel in DOCS:
        p = REPO_ROOT / rel
        if p.exists():
            files.append((p, rel))
    for p in sorted((REPO_ROOT / "docs" / "evidence").rglob("*")):
        if p.is_file() and p.name != ".gitkeep":
            files.append((p, str(p.relative_to(REPO_ROOT))))
    results = REPO_ROOT / "results"
    for folder in sorted(results.iterdir()):
        if not folder.is_dir() or folder.name.startswith("_"):
            continue
        if (
            is_live_v2_run(folder)
            or folder.name.endswith("-tests")
            or folder.name.endswith("-assignment-check")
        ):
            for p in sorted(folder.rglob("*")):
                if p.is_file():
                    files.append((p, str(p.relative_to(REPO_ROOT))))
    for pattern in LOG_PATTERNS:
        for p in sorted((REPO_ROOT / "logs").glob(pattern)):
            files.append((p, str(p.relative_to(REPO_ROOT))))
    return files


def main() -> int:
    settings = Settings.from_env()
    files = collect()
    runs = sorted({arc.split("/")[1] for _, arc in files if arc.startswith("results/")})
    out = REPO_ROOT / "dist" / f"evidence-{datetime.now(UTC):%Y%m%d}.zip"
    out.parent.mkdir(exist_ok=True)

    manifest = []
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for src, arc in files:
            data = src.read_bytes()
            if arc == "logs/producer.log":
                # login_ok / logout echo the key (masked to the last 4
                # characters); a shared bundle carries none of it.
                data = MASKED_KEY.sub(r"\1[redacted]\2", data.decode()).encode()
            z.writestr(arc, data)
            manifest.append(f"{hashlib.sha256(data).hexdigest()}  {len(data):>10}  {arc}")
        z.writestr("MANIFEST.txt", "\n".join(manifest) + "\n")

    # Safety net: no secret may be inside, in any form we can recognise.
    secrets = [s for s in (settings.ws_key, settings.rest_key) if s and len(s) >= 8]
    with zipfile.ZipFile(out) as z:
        for name in z.namelist():
            content = z.read(name)
            if any(s.encode() in content for s in secrets):
                out.unlink()
                print(
                    f"ABORTED: a TraderMade key was found in {name}; bundle deleted",
                    file=sys.stderr,
                )
                return 1

    print(f"wrote {out.relative_to(REPO_ROOT)}: {len(files)} files")
    live = [r for r in runs if not r.endswith(("-tests", "-assignment-check"))]
    print(f"  live v2 result folders: {', '.join(live) or 'none'}")
    print(f"  test reports: {', '.join(r for r in runs if r.endswith('-tests')) or 'none'}")
    print(
        f"  assignment checks: {', '.join(r for r in runs if r.endswith('-assignment-check')) or 'none'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
