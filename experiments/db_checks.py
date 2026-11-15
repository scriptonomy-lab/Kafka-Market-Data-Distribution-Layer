"""
The SQL every experiment uses to check TimescaleDB against Kafka, in one
place, so experiments A/B/C, F (scale-out) and D (replay) all prove
"no loss" and "no seq gap" with exactly the same queries, and /evidence-audit
can quote them.

Ranges are given as {(topic, partition): offset} dicts: `start` is the first
offset of the verified range and `end` is one past the last (Kafka's end
offset), so a partition's expected row count is simply end - start. Without
producer transactions a partition's offsets are contiguous, which is what
makes that an exact completeness test.

All connections here are read-only: an experiment must never change the
data it is judging.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import psycopg

from pipeline.common import TP, Settings, db_conninfo


def connect(settings: Settings) -> psycopg.Connection:
    conn = psycopg.connect(db_conninfo(settings), autocommit=True)
    conn.execute("SET default_transaction_read_only = on")
    return conn


def _ranges(start: dict[TP, int], end: dict[TP, int]) -> list[list]:
    """Four parallel arrays (topic, partition, lo, hi) for unnest()."""
    keys = sorted(start)
    return [
        [t for t, _ in keys],
        [p for _, p in keys],
        [start[k] for k in keys],
        [end[k] for k in keys],
    ]


# The verified offset ranges as a table r(topic, partition, lo, hi), joined to
# the rows that fall inside them.
RANGES_CTE = """
WITH r AS (
    SELECT * FROM unnest(%s::text[], %s::int[], %s::bigint[], %s::bigint[])
        AS r(topic, partition, lo, hi)
)
"""


def stored_per_partition(conn, start: dict[TP, int], end: dict[TP, int]) -> dict[TP, dict]:
    """Expected vs stored rows per topic-partition over [start, end).

    stored     = distinct offsets present (completeness)
    rows       = rows present; rows > stored would mean one message stored
                 twice, which the primary key must make impossible
    """
    sql = (
        RANGES_CTE
        + """
SELECT r.topic, r.partition, count(DISTINCT t.kafka_offset), count(t.kafka_offset)
FROM r LEFT JOIN ticks t
  ON t.topic = r.topic AND t.partition = r.partition
 AND t.kafka_offset >= r.lo AND t.kafka_offset < r.hi
GROUP BY r.topic, r.partition
"""
    )
    out = {}
    for topic, partition, stored, rows in conn.execute(sql, _ranges(start, end)):
        tp = (topic, partition)
        out[tp] = {
            "from_offset": start[tp],
            "to_offset": end[tp],
            "expected": end[tp] - start[tp],
            "stored": stored,
            "rows": rows,
        }
    return dict(sorted(out.items()))


def seq_check(conn, start: dict[TP, int], end: dict[TP, int]) -> dict:
    """Seq contiguity per (producer session, symbol) over the verified rows:
    gaps = (max - min + 1) - distinct seqs, which must be 0."""
    sql = (
        RANGES_CTE
        + """
SELECT t.session_id, t.symbol, t.topic, count(DISTINCT t.seq), min(t.seq), max(t.seq)
FROM r JOIN ticks t
  ON t.topic = r.topic AND t.partition = r.partition
 AND t.kafka_offset >= r.lo AND t.kafka_offset < r.hi
GROUP BY t.session_id, t.symbol, t.topic
"""
    )
    gaps_by_topic: dict[str, int] = defaultdict(int)
    rows_per_symbol: dict[str, int] = defaultdict(int)
    for _session, symbol, topic, n, lo, hi in conn.execute(sql, _ranges(start, end)):
        gaps_by_topic[topic] += (hi - lo + 1) - n
        rows_per_symbol[symbol] += n
    return {
        "seq_gaps": sum(gaps_by_topic.values()),
        "seq_gaps_by_topic": dict(gaps_by_topic),
        "rows_per_symbol": dict(sorted(rows_per_symbol.items())),
    }


def sources(conn, start: dict[TP, int], end: dict[TP, int]) -> list[str]:
    """Distinct `source` values in the range: "simulated" anywhere = not evidence."""
    sql = (
        RANGES_CTE
        + """
SELECT DISTINCT t.source FROM r JOIN ticks t
  ON t.topic = r.topic AND t.partition = r.partition
 AND t.kafka_offset >= r.lo AND t.kafka_offset < r.hi
"""
    )
    return sorted(row[0] for row in conn.execute(sql, _ranges(start, end)))


def verify_range(conn, start: dict[TP, int], end: dict[TP, int]) -> dict:
    """The full no-loss verification, with a per-topic breakdown."""
    per_tp = stored_per_partition(conn, start, end)
    by_topic: dict[str, dict] = {}
    for (topic, _p), d in per_tp.items():
        t = by_topic.setdefault(topic, {"expected": 0, "stored": 0, "lost": 0, "double_stored": 0})
        t["expected"] += d["expected"]
        t["stored"] += d["stored"]
        t["lost"] += d["expected"] - d["stored"]
        t["double_stored"] += d["rows"] - d["stored"]
    seq = seq_check(conn, start, end)
    for topic, gaps in seq["seq_gaps_by_topic"].items():
        by_topic.setdefault(topic, {})["seq_gaps"] = gaps
    return {
        "per_partition": {f"{t}:{p}": d for (t, p), d in per_tp.items()},
        "per_topic": by_topic,
        "expected_total": sum(d["expected"] for d in per_tp.values()),
        "stored_total": sum(d["stored"] for d in per_tp.values()),
        "lost_total": sum(d["expected"] - d["stored"] for d in per_tp.values()),
        "double_stored_total": sum(d["rows"] - d["stored"] for d in per_tp.values()),
        "seq_gaps": seq["seq_gaps"],
        "rows_per_symbol": seq["rows_per_symbol"],
        "sources": sources(conn, start, end),
    }


BATCH_FIELDS = [
    "written_at",
    "instance_id",
    "messages",
    "inserted",
    "duplicates",
    "max_age_ms",
    "min_age_ms",
    "delay_ms",
    "partitions",
]


def batches_since(conn, since: datetime) -> list[dict]:
    """Every Storage write from `since` on, oldest first."""
    sql = (
        f"SELECT {', '.join(BATCH_FIELDS)} FROM storage_batches WHERE written_at >= %s ORDER BY id"
    )
    return [dict(zip(BATCH_FIELDS, row, strict=True)) for row in conn.execute(sql, (since,))]


def write_batches_csv(batches: list[dict], path: Path) -> None:
    """storage_batches.csv for the charts. written_at as ISO-8601 UTC with
    milliseconds (the format of every other timestamp in a run folder);
    partitions as 'topic:p|topic:p'."""
    from pipeline.common import iso_ms

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=BATCH_FIELDS)
        w.writeheader()
        for b in batches:
            w.writerow(
                {
                    **b,
                    "written_at": iso_ms(b["written_at"]),
                    "partitions": "|".join(b["partitions"]),
                }
            )


def rows_in_window(conn, since: datetime, until: datetime) -> list[tuple]:
    """(topic, partition, kafka_offset, kafka_ts, stored_at, instance_id) for
    rows WRITTEN in [since, until), ordered per partition. Experiment F uses
    these to find per-partition processing pauses and the new owner's first
    write after each rebalance."""
    sql = """
SELECT topic, partition, kafka_offset, kafka_ts, stored_at, instance_id
FROM ticks WHERE stored_at >= %s AND stored_at < %s
ORDER BY topic, partition, kafka_offset
"""
    return list(conn.execute(sql, (since, until)))
