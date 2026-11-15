"""The offline partition map must match what Kafka actually does.

pipeline/topology.py:murmur2 is a port of the Java client's partitioner
(which the producer selects with partitioner=murmur2_random). If the port
were wrong, every partition-count decision in topology.toml would be wrong.
"""

from pipeline.topology import expected_partition_map, load_topology, murmur2, partition_for


def test_matches_v1_live_partition_map():
    # run/partition_map.json from the live v1 producer (3 partitions, 2026-11-04):
    # observed from Kafka's own delivery reports, not computed by us.
    assert partition_for("EURUSD", 3) == 2
    assert partition_for("GBPUSD", 3) == 2
    assert partition_for("USDJPY", 3) == 1


def test_murmur2_known_values_from_the_java_client():
    # Values from Kafka's UtilsTest / DefaultPartitioner behaviour:
    # murmur2 of the empty array is 275646681 with seed 0x9747b28c.
    assert murmur2(b"") == 275646681
    assert murmur2(b"21") == (-973932308 & 0xFFFFFFFF)
    assert murmur2(b"foobar") == (-790332482 & 0xFFFFFFFF)


def test_configured_counts_give_every_symbol_its_own_partition():
    for topic, symbols in expected_partition_map(load_topology()).items():
        parts = list(symbols.values())
        assert len(set(parts)) == len(parts), f"{topic}: symbols share a partition {symbols}"


def test_documented_v2_map():
    # The map written in the README; if topology.toml changes, update both.
    assert expected_partition_map(load_topology()) == {
        "marketdata.fx.quotes.v1": {"EURUSD": 5, "GBPUSD": 2, "USDJPY": 1},
        "marketdata.crypto.quotes.v1": {"BTCUSD": 3, "ETHUSD": 0},
        "marketdata.equities.us.quotes.v1": {"AAPL": 3, "MSFT": 0, "NVDA": 5},
    }
