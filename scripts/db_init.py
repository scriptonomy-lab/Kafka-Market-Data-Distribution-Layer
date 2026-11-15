"""
Apply db/migrations/*.sql to TimescaleDB, once each, in file-name order.

Role in the architecture
------------------------
Schema changes are a separate, one-shot step (the `db-init` Compose service,
which Storage waits for with `service_completed_successfully`). Storage
instances never run DDL: with two or three of them starting at the same
moment, "CREATE TABLE IF NOT EXISTS" plus create_hypertable() in each would
race. Here there is exactly one writer of the schema.

Idempotent: every applied file is recorded in `schema_migrations`, so a
second run does nothing. A Postgres advisory lock also serialises two
db_init runs, in case one is started by hand while the service runs.

Each file runs in its own transaction together with its schema_migrations
row: a file that fails half way leaves no trace and is retried next time.

Run:  make db-init       (or, in Docker: docker compose run --rm db-init)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import psycopg

from pipeline.common import REPO_ROOT, Settings, db_conninfo

MIGRATIONS_DIR = REPO_ROOT / "db" / "migrations"
# Any constant works; it only has to be the same for every db_init run.
ADVISORY_LOCK_ID = 7_220_001
CONNECT_ATTEMPTS = 30  # x 2 s: the database may still be starting


def connect(settings: Settings) -> psycopg.Connection:
    """Connect, retrying while the server is still starting up."""
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return psycopg.connect(db_conninfo(settings), autocommit=True)
        except psycopg.OperationalError as exc:
            if attempt == CONNECT_ATTEMPTS:
                raise
            print(f"database not ready ({type(exc).__name__}), retry {attempt}", flush=True)
            time.sleep(2)
    raise AssertionError("unreachable")


def pending(conn: psycopg.Connection, files: list[Path]) -> list[Path]:
    applied = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
    return [f for f in files if f.stem not in applied]


def main() -> int:
    settings = Settings.from_env()
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    with connect(settings) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
        try:
            todo = pending(conn, files)
            for f in todo:
                with conn.transaction():
                    conn.execute(f.read_text())
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (f.stem,))
                print(f"applied {f.name}", flush=True)
            if not todo:
                print(f"schema up to date ({len(files)} migrations)", flush=True)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
    return 0


if __name__ == "__main__":
    sys.exit(main())
