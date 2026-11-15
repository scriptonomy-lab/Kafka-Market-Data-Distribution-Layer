"""
pipeline — one TraderMade live feed, routed into three asset-class Kafka
topics (FX, crypto, US stocks) and consumed by four independent groups.

Module map (read them in this order the first time):

    topology.py   config/topology.toml: topics, symbols, instrument metadata, murmur2
    common.py     shared plumbing: settings, logging, Kafka configs, lag sampling
    normalize.py  pure functions: raw TraderMade frame -> internal event dict
    rest.py       the three narrow TraderMade REST calls (off the hot path)
    producer.py   TraderMade WebSocket V2 -> normalize -> Kafka (one process)
    pricing.py    consumer group "pricing-svc": latest bid/ask/mid per symbol
    risk.py       consumer group "risk-svc": mark-to-market of a synthetic book
    storage.py    consumer group "storage-svc": idempotent TimescaleDB persistence,
                  N instances (scale-out with cooperative rebalancing)
    monitor.py    consumer group "monitor-svc": lag/state of every group

Every service is a standalone script with its own main loop. Nothing imports
another service; they only share the Kafka topics. That is the whole point.
"""
