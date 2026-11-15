# Experiment D (replay) — 20261113T200311Z

Source: live TraderMade feed.

| Measure | Value |
| --- | --- |
| New consumer group | `replay-audit-20261113T200311Z` |
| Retained messages, all topics (expected) | 58269 |
| Messages replayed | 58269 |
| Offset holes | 0 |
| Replay time (s) | 2.19 |
| Replay rate (msg/s) | 26630.4 |
| Compared with Storage up to its committed offsets | 58242 replayed vs 58242 stored |
| Replay matches Storage | True |

| Topic | Replayed | Replayed in compared range | Stored in TimescaleDB | Match |
| --- | --- | --- | --- | --- |
| marketdata.crypto.quotes.v1 | 28096 | 28095 | 28095 | True |
| marketdata.equities.us.quotes.v1 | 12002 | 11996 | 11996 | True |
| marketdata.fx.quotes.v1 | 18171 | 18151 | 18151 | True |

| Symbol | Events |
| --- | --- |
| AAPL | 3105 |
| BTCUSD | 14175 |
| ETHUSD | 13921 |
| EURUSD | 4718 |
| GBPUSD | 5985 |
| MSFT | 3005 |
| NVDA | 5892 |
| USDJPY | 7468 |
