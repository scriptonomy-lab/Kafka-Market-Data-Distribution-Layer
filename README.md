# Scalable Multi-Asset Market Data Distribution with TraderMade and Kafka

A working prototype of an institutional market data distribution layer. One
TraderMade WebSocket V2 session is normalized once and routed into three
asset-class Kafka topics (FX, crypto, US stocks). Four independent workloads
(Pricing, Risk, Storage and Monitoring) consume them at their own pace,
each in its own container, and Storage scales out across several instances.
Scripted experiments stop, crash and scale the Storage group under live data,
measuring workload isolation, event recovery, and rebalance behaviour.

> **Status:** The v2 implementation is complete and covered by unit, protocol,
> and integration tests against Kafka and TimescaleDB. The measurements below
> come from live runs performed on November 13, 2026 and are tied to the
> corresponding `results/<run-id>/summary.json` files.

---

## Architecture and data flow

```
             TraderMade WebSocket V2  (wss://stream.tradermade.com/feedAdv)
                    │  ONE session: login → login_ok → subscribe(8 symbols, send_last) → sub_ack
                    ▼
        ┌────────────────────────────────┐
        │ producer (container)           │  normalize → validate → stamp seq/session_id
        │ pipeline/producer.py           │  route by asset class (config/topology.toml)
        └──────┬──────────┬──────────┬───┘  key = symbol · idempotent · acks=all · murmur2
               ▼          ▼          ▼
   marketdata.fx    marketdata.crypto   marketdata.equities.us      Kafka 4.3.1 (KRaft, 1 broker)
   .quotes.v1       .quotes.v1          .quotes.v1                   6 partitions per topic
   EURUSD GBPUSD    BTCUSD ETHUSD       AAPL MSFT NVDA
   USDJPY  (7 d)    (3 d)               (14 d)
       │  │  │  │       │  │  │  │          │  │  │  │
       ▼  ▼  ▼  ▼   (every group reads the topics it needs, at its own pace)
   pricing-svc     risk-svc            storage-svc × N                 monitor-svc
   all topics      FX+crypto+stocks    all topics, cooperative-sticky  all topics
   latest prices   synthetic book MTM  ──► TimescaleDB hypertable      lag per topic,
   auto-commit 1 s auto-commit 1 s         write → commit → offsets    members, rebalances
```

Why one producer: in the live tests used for this build, opening another
session with the same key returned `duplicate_login`. The architecture
therefore keeps upstream connectivity in one producer and distributes the
feed internally.

Why downstream workloads do not directly slow the WebSocket: `produce()`
writes into Kafka's local producer queue, while consumers read from the broker
independently. Slow Pricing, Risk, or Storage consumers therefore do not
directly back-pressure the TraderMade feed handler. Broker unavailability or
a saturated producer queue can still affect ingestion, so the producer handles
and reports those conditions explicitly.

## Quick Start

Prerequisites: Docker (Docker Desktop or OrbStack), Python 3.11+ for the
host-side tools, `make`, `tmux` (for `make demo`), a TraderMade streaming key
and REST key.

```bash
make setup                 # .venv for host tools, .env from the template, random DB password
# edit .env: TRADERMADE_WS_KEY and TRADERMADE_REST_KEY
make up                    # build the image, start the whole stack (storage x2)
make ps                    # every service running; kafka + timescaledb healthy
make seed-book             # risk book entry prices from REST /live
make preflight             # PASS/FAIL readiness check
make watch-pricing         # live table (detach: Ctrl-p Ctrl-q); also watch-risk, watch-monitor
make scale N=3             # a third Storage instance; watch the rebalance in watch-monitor
make test && make test-integration
```

Offline development: `make up-dev` runs the same stack against the bundled
TraderMade simulator (dummy key; results land in `results/_dev/`, never
evidence).

### Docker services

| Service | Image | Role | Restart | Notes |
| --- | --- | --- | --- | --- |
| `kafka` | `apache/kafka:4.3.1` | broker + controller (KRaft) | - | healthcheck; `kafka:19092` inside, `localhost:9092` for host tools |
| `timescaledb` | `timescale/timescaledb:2.31.0-pg17` | Storage's database | - | `pg_isready` healthcheck; published on 127.0.0.1 only |
| `kafka-init` | app image | create the three topics from `topology.toml` | no (one-shot) | consumers start after it exits 0 |
| `db-init` | app image | apply `db/migrations/*.sql` | no (one-shot) | Storage starts after it exits 0; Storage never runs DDL |
| `producer` | app image | TraderMade → Kafka | unless-stopped | the ONLY container with the WebSocket key |
| `pricing`, `risk`, `monitor` | app image | consumers with live tables | unless-stopped | `make watch-*` attaches to the table |
| `storage` | app image | consumer → TimescaleDB | no | scalable (`--scale storage=N`), no fixed name; 30 s stop grace |
| `seed-book` | app image | REST /live → `config/positions.json` | no (profile `tools`) | the ONLY container with the REST key |
| `kafka-ui` | `kafbat/kafka-ui:v1.5.0` | optional web UI | - | profile `ui` (`make ui`) |
| `fake-feed` | app image | offline simulator | - | profile `dev` (`make up-dev`) |

One application image (`python:3.12.15-slim-trixie`, non-root, arm64 and
amd64) runs every Python service with a different command. Secrets reach a
container only as Compose secrets (files under `/run/secrets/`), so they are
not in the image, the compose file, `docker inspect` or the logs.

## Topic and partition design

One WebSocket session feeds three topics, one per asset class. The producer
routes each event by the instrument's configured asset class
([`config/topology.toml`](config/topology.toml)), never by the shape of the
symbol, and keys every message by the internal symbol.

| Topic | Asset class | Symbols (TraderMade WS code) | Partitions | Retention | Key |
| --- | --- | --- | --- | --- | --- |
| `marketdata.fx.quotes.v1` | fx | EURUSD, GBPUSD, USDJPY | 6 | 7 days | symbol |
| `marketdata.crypto.quotes.v1` | crypto | BTCUSD, ETHUSD | 6 | 3 days | symbol |
| `marketdata.equities.us.quotes.v1` | us_stocks | AAPL (`AAPL.US`), MSFT (`MSFT.US`), NVDA (`NVDA.US`) | 6 | 14 days | symbol |

All topics: replication factor 1 (single broker), `cleanup.policy=delete`,
created by `make topics` from the same file. The WebSocket codes were checked
against the live service on 2026-11-13: `sub_ack` accepted the codes above and
rejected `AAPL`, `MSFT`, `NVDA` and `NVDAUSD` as invalid.

**Why separate topics.** A topic is the unit Kafka configures, scales and
secures. Splitting by asset class buys four independent knobs that one shared
topic cannot offer:

- **Retention per asset class.** Crypto trades 24/7 and produces the most
  data per day, so it keeps 3 days. US equities tick for about 6.5 hours per
  weekday and keep 14 days, which spans two weekends. FX keeps the 7-day replay
  window from v1.
- **Partitions per asset class.** Each topic's partition count is chosen for
  its own symbols (below), and can grow without re-hashing the other classes.
- **Consumers per asset class.** Risk subscribes only to the topics its book
  needs; Pricing, Storage and Monitoring read all three. A new equities-only
  service would read one topic and never see FX or crypto traffic.
- **Access per asset class.** In production, ACLs can grant one desk FX only.
  This prototype runs without authentication; that is a stated limitation.

**Symbol → partition map.** Partition = murmur2(key) mod partitions, the Java
client's hash (`partitioner=murmur2_random`). `make partition-map` computes it
from `topology.toml`; `make partition-map ARGS=--check` compares it with the
map the producer observed from Kafka's delivery reports (`run/partition_map.json`).

| Topic | Map (6 partitions) | Empty partitions |
| --- | --- | --- |
| fx | EURUSD → 5, GBPUSD → 2, USDJPY → 1 | 0, 3, 4 |
| crypto | BTCUSD → 3, ETHUSD → 0 | 1, 2, 4, 5 |
| us_stocks | AAPL → 3, MSFT → 0, NVDA → 5 | 1, 2, 4 |

Six is the smallest count at which FX and US stocks put every symbol on its
own partition (`make partition-map ARGS=--scan`; crypto would need only 2, and
uses 6 for one consistent setting). v1's 3 partitions put EURUSD and GBPUSD
together on partition 2. With key = symbol, at most one partition per
symbol carries data, so 8 of the 18 partitions are used and a consumer group
can never usefully run more than 8 instances in parallel. Empty partitions are
expected, not a fault.

## Consumer-group design

| Group | Subscribes to | Instances | Commit policy | `auto.offset.reset` | On restart |
| --- | --- | --- | --- | --- | --- |
| `pricing-svc` | all three topics | 1 | auto, every 1 s | latest | resumes at committed offset; latest-state, so re-reading a few seconds is harmless |
| `risk-svc` | the topics its book needs (its symbols + USD conversion pairs) | 1 | auto, every 1 s | latest | same; keeps its own price map (no dependency on Pricing) |
| `storage-svc` | all three topics | N (default 2) | manual: one TimescaleDB transaction per batch → `store_offsets` → commit every 5 s (asynchronous) and on revoke/shutdown (synchronous) | earliest | resumes at last commit; redelivered rows ignored by the primary key |
| `monitor-svc` | all three topics | 1 | auto | latest | rebuilds its view from Kafka in one poll |

Every group uses `partition.assignment.strategy=cooperative-sticky`
(incremental rebalancing: only partitions that move are revoked) and
`session.timeout.ms=10000`. A member's `client.id` is `<group>-<container
hostname>`, so `kafka-consumer-groups --describe --members` names the
container that owns each partition.

### Storage scale-out

Storage instances share the group `storage-svc`; Kafka splits the 18
partitions of the three topics between them. The rebalance callbacks:
`on_assign` logs each newly assigned topic-partition with its committed
offset; `on_revoke` commits the stored offsets first, then logs; `on_lost`
only logs (another member may already own the partitions, so it must not
commit). `instance_id` (the container hostname) is on every log line, every
row and every batch record; each instance logs to `logs/storage-<id>.log`.

Because key = symbol, only 8 partitions carry data. Whether every instance
still owns data with 2 or 3 instances is decided by the assignor, not the
hash, so it is checked against the real broker by
`scripts/assignment_check.py` (`make assignment-check ARGS=--save`). On
2026-11-13 it passed 5 of 5 trials of experiment F's membership sequence on
throw-away topics with the same partition counts: with 2 members each owned
9 partitions (3 and 5 with data), with 3 members 6 each (2, 2 and 4 with
data), and after a clean leave 9 each (4 and 4 with data); on every join
only the partitions that moved were revoked
(`20261113T180251Z-assignment-check`).

If the database is unreachable, Storage stores no offset for the batch, seeks
back to its first offsets and retries with backoff (1 → 30 s), staying in the
group; an offset is never committed for a row that was not written.

## Event schema

```json
{"schema_version":2,"type":"quote","symbol":"AAPL","asset_class":"us_stocks","provider_symbol":"AAPL.US",
 "bid":230.10,"ask":230.12,"mid":230.11,"bid_size":100,"ask_size":100,
 "timestamp":"2026-11-16T15:05:12.713Z","ingest_ts":"2026-11-16T15:05:12.741Z",
 "seq":18342,"session_id":"7f3c2a1b9e0d","source":"tradermade"}
```

(Illustrative layout, not a recorded tick.) `seq` is stamped by the producer
per symbol (the feed has none) so consumers can detect gaps; `session_id`
changes when the producer restarts. v2 added `asset_class` and
`provider_symbol`. The consumers in this repository ignore fields they do not
use, so adding those fields did not break the existing consumer implementations
and the topic names keep `.v1`.

## Design decisions

| Decision | Alternative rejected | Reason |
| --- | --- | --- |
| One topic per asset class, one group per workload (v2) | One shared topic (v1) / one topic per consumer | Per-class retention, partitions, subscriptions and access; Kafka already fans out to groups, so per-consumer topics would duplicate data and couple the producer to its consumers |
| Route by configured asset class, key = internal symbol | Infer from symbol shape / key by provider code | `BTCUSD` looks like an FX pair; provider codes (`AAPL.US`) must not decide partitions |
| 6 partitions per topic | 3 (v1) | Smallest count at which every FX and stock symbol gets its own partition (murmur2), so they can be processed in parallel |
| Cooperative-sticky assignor | Eager (range/roundrobin) | A join or leave only pauses the partitions that move; measured in experiment F |
| One image, one container per service, Compose secrets | Host processes (v1) / secrets in env | Isolation and one-command start-up; keys reach only the container that needs them |
| Producer stamps `seq` + `session_id` | Trust provider timestamps for ordering | Timestamps can tie or move; a counter makes gaps and duplicates countable |
| `send_last=true` + snapshot de-dup | Wait for the next tick after reconnect | Consumers get a price immediately; identical snapshots are not republished |
| Storage commits after the write, PK on offset | Auto-commit | Auto-commit can mark rows done before they are written (loss on crash) |
| Offsets committed every 5 s, rows every batch | Commit per batch | Fewer broker round-trips; makes redelivery observable in experiment B |
| Periodic offset commit asynchronous; sync only on revoke and shutdown | Synchronous commit every 5 s | A sync commit blocks the consume loop until the broker answers; during a rebalance that stalled partitions that never moved (found in a simulator rehearsal of experiment F). A failed async commit is carried by the next one |
| "Caught up" = committed everything older than commit interval + 5 s | Lag below a fixed number | Storage's committed lag is a sawtooth that grows with rate and instance count; a fixed threshold is met by chance or never |
| JSON on the wire | Avro/Protobuf + schema registry | Readable in an article; registry is the named production step |
| TimescaleDB hypertable on Kafka time (v2) | SQLite (v1) | Several Storage instances must write concurrently from separate containers; a single-file database cannot be shared across them. Only the tick table is used: no continuous aggregates or Grafana, so this does not rebuild TraderMade's TimescaleDB tutorial |
| PK (topic, partition, kafka_offset, kafka_ts) | PK without kafka_ts | A hypertable's unique keys must include its time column; a redelivered message has the identical CreateTime, so duplicates are still caught |
| DB down: store no offset, seek back, retry with backoff | Skip the batch / commit anyway | An offset is never committed for a row that was not written |

## Failure and recovery findings

> Measurements below come from live v2 runs, with each value tied to the
> corresponding `results/<run-id>/summary.json`.

Conditions for all three runs: 2026-11-13, 14:44–15:03 New York
(19:44–20:03 UTC), US stock, FX and crypto markets open; live TraderMade
WebSocket V2 feed, one session, 8 symbols; everything on one host (macOS,
Docker via OrbStack); one Kafka broker; TimescaleDB in a container. Storage
commits offsets every 5 s; `session.timeout.ms` 10 s.

### Experiment F: scale-out and rebalance (`make experiment-f`)

Run `20261113T194443Z-expF-scale`: 2 Storage instances steady, then scale to
3, then `docker stop` one, then `docker kill -s KILL` another; 120 s per step.

| Measure | Scale-out 2 → 3 | Clean stop 3 → 2 | Crash (SIGKILL) 2 → 1 |
| --- | --- | --- | --- |
| Action → group STABLE with every partition owned by a live instance (s) | 7.4 | 0.42 | 11.77 |
| of which: action → first rebalance callback (s) ¹ | 4.19 | 0.27 | 11.5 |
| of which: first callback → done (s) | 3.21 | 0.15 | 0.28 |
| Partitions moved (all / carrying data) | 6 / 3 | 6 / 3 | 9 / 4 |
| First write by the new owner, slowest moved partition (s) | 7.247 | 0.612 | 11.753 |
| Max processing delay, moved partitions (ms) ² | 3,197.4 | 289.7 | 11,746.7 |
| Max processing delay, unmoved partitions: step vs steady (ms) ² | 470.5 vs 265.0 | 269.0 vs 260.6 | 437.7 vs 267.1 |
| Unmoved partitions kept processing ³ | PASS | PASS | PASS |
| Data-carrying partitions per live instance | 2 / 3 / 3 | 4 / 4 | 8 |
| Duplicates suppressed during the step | 0 | 0 | 25 |

Over the whole run (15,620 messages: crypto 7,408 · FX 4,922 · US stocks
3,290) no message was lost, none was stored twice, and there were 0 seq gaps;
the steady step had 0 duplicates. The stopped and the killed containers
stayed down (no restart policy). Charts: `ownership.png` (who owned each
topic-partition, second by second), `lag_by_topic.png`, `throughput.png`.

¹ Scale-out: the new container must start Python and join the group. Crash:
the broker only notices a SIGKILLed member after `session.timeout.ms`
(10 s); until then it still owns its partitions and nobody processes them.
² Processing delay = `stored_at` − Kafka timestamp, per message, in the 60 s
after the action; a gap alone cannot tell a pause from a quiet symbol.
³ Every unmoved data-carrying partition stayed within max(2 × its steady
maximum, steady maximum + 1 s). With cooperative-sticky assignment the
partitions that did not move kept being written during each rebalance.

### Experiment B: crash and recovery of the whole Storage group (`make experiment-b`)

Run `20261113T195345Z-expB-crash`: both Storage instances killed together
(SIGKILL), 300 s outage, the same containers restarted; baseline / outage /
settle 120 / 300 / 120 s.

| Measure | Value |
| --- | --- |
| Ingress during baseline (msg/s) | 28.783 (crypto 11.592 · FX 10.325 · US stocks 6.867) |
| Storage lag at restart (messages) | 11,267 (crypto 4,562 · FX 4,228 · US stocks 2,477) |
| Catch-up to live data: first batch with event age ≤ 2 s (s) | 1.21, after writing 11,305 messages |
| Catch-up: committed lag back to its baseline maximum (155) (s) ⁴ | 4.02 |
| Pricing / Risk / Monitor max lag, outage vs baseline (messages) ⁵ | 78 vs 57 · 83 vs 56 · 84 vs 61 |
| Events lost | 0 of 19,320 (crypto 8,096 · FX 6,906 · US stocks 4,318) |
| Duplicates suppressed (redelivered after the crash, ignored by the primary key) | 73 |
| Seq gaps | 0 |
| Resumed at committed offset | PASS: all 8 data-carrying partitions resumed exactly at their committed offset |
| Killed containers auto-restarted | no (exit 137, restart count 0) |

⁴ Measured by the runner's 1 s polling loop. ⁵ All within the run's
isolation threshold (outage max ≤ max(2 × baseline max, baseline max + 20)),
per topic as well; Pricing, Risk and Monitor auto-commit every second, so
their lag reflects one to two seconds of traffic.

### Experiment D: replay of all three topics (`make replay`)

A brand-new consumer group replayed 58,269 of 58,269 retained messages from
the three topics (crypto 28,096 · FX 18,171 · US stocks 12,002) in 2.19
seconds on the test host, with 0 offset holes. For the 58,242 offsets Storage
had committed at that moment, TimescaleDB holds exactly the same rows, per
topic (`20261113T200311Z-expD-replay`). The observed replay rate is specific
to this local environment and should not be treated as a Kafka throughput
benchmark.

## Known limitations

- Single broker, replication factor 1: a broker failure loses availability (and possibly data).
- Everything runs on one host; the measured producer→consumer age is a same-clock Kafka hop, not feed latency.
- US stocks only tick in US market hours (09:30–16:00 New York); outside them the equities topic is quiet and its freshness grows. Exchange holidays are not modelled.
- With key = symbol, 8 of 18 partitions carry data and a group can use at most 8 instances in parallel.
- The synthetic Risk book used in this run did not include seeded MSFT or NVDA positions, so those symbols were not part of the Risk workload.
- Kafka can replay only events that reached Kafka. Any gap that occurs upstream during a WebSocket interruption requires a separate upstream recovery or historical-backfill mechanism.
- Prices are floats; production systems carry decimals or integer ticks.
- JSON without a schema registry; schema evolution is additive within a topic version.
- No authentication or ACLs on Kafka or between containers; the database is published on 127.0.0.1 only.
- Cross-currency pairs are not supported by the synthetic risk book.

## Taking it further

Not built here, deliberately; each is the named production step:

- **Three brokers**, replication factor 3, `min.insync.replicas=2`: survive a broker loss.
- **Static membership (KIP-345)**: a restarted instance with the same `group.instance.id` gets its partitions back without a rebalance.
- **The KIP-848 consumer protocol** (`group.protocol=consumer`): broker-side, fully incremental assignment.
- **Schema registry** with Avro or Protobuf: enforced, evolvable event schemas.
- **TimescaleDB continuous aggregates** for bars and other derived views, and Grafana on top of them.
- ACLs per topic, so access follows the asset-class split.

## Reproduce

```bash
make setup && $EDITOR .env
make up && make test && make test-integration
make seed-book && make preflight
make experiment-f && make scale N=2 && make experiment-b && make replay   # US market hours
```
