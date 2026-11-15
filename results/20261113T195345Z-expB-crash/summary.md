# Experiment B (crash) — 20261113T195345Z-expB-crash

Source: live TraderMade WebSocket V2 feed.

All numbers below were computed by `experiments/failure_recovery.py` from `lag.csv` and TimescaleDB. None were typed by hand.

## Checks

| Check | Result |
| --- | --- |
| other groups unaffected | PASS |
| no events lost | PASS |
| no message stored twice | PASS |
| seq gaps zero | PASS |
| resumed at committed offset | PASS |
| storage lag grew during outage | PASS |
| stopped instances not auto restarted | PASS |
| redelivery observed after crash | PASS |

## Run

| Item | Value |
| --- | --- |
| Storage instances | 2 (e8654079371c, 3347c5d76386) |
| Phase `setup` started | 2026-11-13T19:53:45.850Z |
| Phase `baseline` started | 2026-11-13T19:53:56.940Z |
| Phase `outage` started | 2026-11-13T19:55:59.050Z |
| Phase `catchup` started | 2026-11-13T20:01:01.452Z |
| Phase `settle` started | 2026-11-13T20:01:05.610Z |
| Phase `verify` started | 2026-11-13T20:03:10.701Z |
| Baseline / outage / settle (s) | 120 / 300 / 120 |
| Ingress during baseline (msg/s) | 28.783 |
| Ingress, fx (msg/s) | 10.325 |
| Ingress, crypto (msg/s) | 11.592 |
| Ingress, equities.us (msg/s) | 6.867 |
| Slow-mode delay (ms/msg) | - |

## Storage outage and recovery

| Measure | Value |
| --- | --- |
| Storage max lag during baseline (messages) | 155 |
| Storage lag at restart (messages) | 11267 |
| Storage lag at restart, crypto | 4562 |
| Storage lag at restart, equities.us | 2477 |
| Storage lag at restart, fx | 4228 |
| Catch-up time, committed lag back to baseline (s) | 4.02 |
| Catch-up time, first batch with event age ≤ 2000 ms (s) | 1.21 |
| Messages written during catch-up | 11305 |
| Catch-up processing rate (msg/s) | 9343.0 |

## Other consumers during the outage (max lag, messages)

| Group | Topic | Baseline | Outage | Isolation |
| --- | --- | --- | --- | --- |
| pricing-svc | all | 57 | 78 | PASS |
| pricing-svc | fx | 23 | 21 | PASS |
| pricing-svc | crypto | 42 | 57 | PASS |
| pricing-svc | equities.us | 10 | 13 | PASS |
| risk-svc | all | 56 | 83 | PASS |
| risk-svc | fx | 23 | 21 | PASS |
| risk-svc | crypto | 41 | 62 | PASS |
| risk-svc | equities.us | 10 | 13 | PASS |
| monitor-svc | all | 61 | 84 | PASS |
| monitor-svc | fx | 30 | 23 | PASS |
| monitor-svc | crypto | 42 | 62 | PASS |
| monitor-svc | equities.us | 15 | 14 | PASS |

## No-loss verification per topic (Kafka offsets vs TimescaleDB rows)

| Topic | Expected | Stored | Lost | Seq gaps |
| --- | --- | --- | --- | --- |
| crypto | 8096 | 8096 | 0 | 0 |
| equities.us | 4318 | 4318 | 0 | 0 |
| fx | 6906 | 6906 | 0 | 0 |

Total expected 19320, stored 19320, lost 0, stored twice 0, seq gaps 0, duplicates suppressed 73.

| Topic-partition | From offset | To offset | Expected | Stored |
| --- | --- | --- | --- | --- |
| crypto:0 | 10514 | 13919 | 3405 | 3405 |
| crypto:3 | 9434 | 14125 | 4691 | 4691 |
| equities.us:0 | 1825 | 2986 | 1161 | 1161 |
| equities.us:3 | 1920 | 3093 | 1173 | 1173 |
| equities.us:5 | 3888 | 5872 | 1984 | 1984 |
| fx:1 | 4575 | 7438 | 2863 | 2863 |
| fx:2 | 3847 | 5971 | 2124 | 2124 |
| fx:5 | 2785 | 4704 | 1919 | 1919 |

## Resume point (first offset read after restart vs committed before)

| Topic-partition | Committed before restart | First offset read |
| --- | --- | --- |
| marketdata.crypto.quotes.v1:0 | 10871 | 10871 |
| marketdata.crypto.quotes.v1:3 | 10430 | 10430 |
| marketdata.equities.us.quotes.v1:0 | 2036 | 2036 |
| marketdata.equities.us.quotes.v1:3 | 2148 | 2148 |
| marketdata.equities.us.quotes.v1:5 | 4279 | 4279 |
| marketdata.fx.quotes.v1:1 | 5120 | 5120 |
| marketdata.fx.quotes.v1:2 | 4248 | 4248 |
| marketdata.fx.quotes.v1:5 | 3097 | 3097 |

Charts: `lag.png` (all groups), `isolation.png`, `lag_by_topic.png`, `catchup.png`.
