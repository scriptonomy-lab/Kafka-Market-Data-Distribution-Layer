# Experiment F (scale-out and rebalance) — 20261113T194443Z-expF-scale

Source: live TraderMade WebSocket V2 feed.

All numbers were computed by `experiments/scale_rebalance.py` from the sampler CSVs, `actions.json` and TimescaleDB. None were typed by hand.

Storage group `storage-svc`, assignor cooperative-sticky, session.timeout.ms 10000, 120 s per step. Original instances e8654079371c, 3347c5d76386; clean stop e8654079371c, crash 3347c5d76386.

## Checks

| Check | Result |
| --- | --- |
| no events lost | PASS |
| no message stored twice | PASS |
| seq gaps zero | PASS |
| duplicates zero on clean steps | PASS |
| rebalance completed every step | PASS |
| every live instance owns data | PASS |
| unmoved partitions kept processing | PASS |
| stopped and killed not auto restarted | PASS |

## Steps

| Measure | scale_out | clean_stop | crash |
| --- | --- | --- | --- |
| Action | scale storage=3 | stop tradermade-kafka-distribution-storage-1 | kill tradermade-kafka-distribution-storage-2 |
| Action started (UTC) | 19:47:04.022 | 19:49:08.287 | 19:51:12.618 |
| Action -> rebalance done: STABLE, every partition owned by a live member (s) | 7.4 | 0.42 | 11.77 |
| of which: action -> first rebalance callback in any instance (s) | 4.19 | 0.27 | 11.5 |
| of which: first callback -> rebalance done (s) | 3.21 | 0.15 | 0.28 |
| Partitions moved (all / carrying data) | 6 / 3 | 6 / 3 | 9 / 4 |
| First write by new owner, slowest moved data partition (s) | 7.247 | 0.612 | 11.753 |
| Max processing delay, moved data partitions (ms) | 3197.4 | 289.7 | 11746.7 |
| Unmoved data partitions kept processing | PASS | PASS | PASS |
| Data partitions owned per live instance | 3347c5d76386: 2, 71c9e03b6582: 3, e8654079371c: 3 | 3347c5d76386: 4, 71c9e03b6582: 4 | 71c9e03b6582: 8 |
| Duplicates suppressed during the step | 0 | 0 | 25 |
| Messages written during the step | 3625 | 3764 | 4016 |

Steady step: 3804 messages written by 3347c5d76386, e8654079371c, duplicates 0.

## Unmoved partitions during each rebalance (processing delay = stored_at - Kafka timestamp)

| Step | Topic-partition | Steady max (ms) | Step max (ms) | Limit (ms) | Kept processing |
| --- | --- | --- | --- | --- | --- |
| scale_out | marketdata.crypto.quotes.v1:3 | 267.6 | 403.5 | 1267.6 | PASS |
| scale_out | marketdata.equities.us.quotes.v1:0 | 266.2 | 260.1 | 1266.2 | PASS |
| scale_out | marketdata.fx.quotes.v1:1 | 262.4 | 456.5 | 1262.4 | PASS |
| scale_out | marketdata.fx.quotes.v1:2 | 264.7 | 464.5 | 1264.7 | PASS |
| scale_out | marketdata.fx.quotes.v1:5 | 265.0 | 470.5 | 1265.0 | PASS |
| clean_stop | marketdata.crypto.quotes.v1:0 | 265.5 | 257.8 | 1265.5 | PASS |
| clean_stop | marketdata.crypto.quotes.v1:3 | 267.6 | 265.3 | 1267.6 | PASS |
| clean_stop | marketdata.equities.us.quotes.v1:0 | 266.2 | 257.7 | 1266.2 | PASS |
| clean_stop | marketdata.equities.us.quotes.v1:3 | 260.6 | 269.0 | 1260.6 | PASS |
| clean_stop | marketdata.equities.us.quotes.v1:5 | 267.1 | 268.9 | 1267.1 | PASS |
| crash | marketdata.crypto.quotes.v1:0 | 265.5 | 366.7 | 1265.5 | PASS |
| crash | marketdata.equities.us.quotes.v1:3 | 260.6 | 265.3 | 1260.6 | PASS |
| crash | marketdata.equities.us.quotes.v1:5 | 267.1 | 437.7 | 1267.1 | PASS |
| crash | marketdata.fx.quotes.v1:2 | 264.7 | 267.9 | 1264.7 | PASS |

## No-loss verification per topic (Kafka offsets vs TimescaleDB rows)

| Topic | Expected | Stored | Lost | Seq gaps |
| --- | --- | --- | --- | --- |
| crypto | 7408 | 7408 | 0 | 0 |
| equities.us | 3290 | 3290 | 0 | 0 |
| fx | 4922 | 4922 | 0 | 0 |

Total expected 15620, stored 15620, lost 0, stored twice 0, seq gaps 0.

Charts: `ownership.png` (who owned each topic-partition), `lag_by_topic.png`, `throughput.png`.
Raw: `actions.json`, `ownership.csv`, `group_states.csv`, `partition_activity.csv`, `describe_*_before/after.txt`.
