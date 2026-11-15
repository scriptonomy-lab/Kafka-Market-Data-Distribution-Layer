"""
The market data topology: which topics exist, which symbols each carries,
and what every instrument is. Loaded from config/topology.toml.

Role in the architecture
------------------------
v1 had one FX topic and could get away with reading meaning out of symbol
strings ("ends with JPY", "the first three letters are the base currency").
With three asset classes that stops being safe: "AAPL" has no currency
pair, and a provider code like "AAPL.US" must not decide which partition a
tick lands on. So every service asks THIS module instead:

    topology.instrument("USDJPY").quote_currency   -> "JPY"   (Risk)
    topology.topic_for("BTCUSD")                    -> "marketdata.crypto.quotes.v1"  (producer)
    topology.by_ws_symbol["EURUSD"]                 -> Instrument (normalizer)
    topology.topic_names                            -> all three topics (consumers)

It also holds Kafka's murmur2 partitioner (a faithful port of the Java
client's Utils.murmur2), so the expected symbol -> partition map can be
computed offline and compared with what the producer actually observes.

Everything here is pure: no network, no Kafka. Validation errors are raised
at start-up with a message that names the bad entry, so a typo in the TOML
stops a service immediately instead of silently misrouting ticks.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TOPOLOGY_FILE = Path(__file__).resolve().parent.parent / "config" / "topology.toml"

# The asset classes this pipeline knows how to route. Also the TOML table keys.
ASSET_CLASSES = ("fx", "crypto", "us_stocks")


class TopologyError(ValueError):
    """config/topology.toml is inconsistent. The message says where."""


@dataclass(frozen=True)
class Topic:
    asset_class: str
    name: str
    partitions: int
    retention_ms: int
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class Instrument:
    symbol: str  # internal symbol: Kafka key and event "symbol"
    asset_class: str
    ws_symbol: str  # WebSocket subscription code and the tick's "s"
    rest_symbol: str | None  # REST /live code; None when REST has no code for it
    base_currency: str | None  # None for stocks (quantity is shares)
    quote_currency: str
    price_decimals: int
    pip_or_tick_size: float


@dataclass(frozen=True)
class Topology:
    symbol_limit: int
    topics: dict[str, Topic]  # asset class -> Topic
    instruments: dict[str, Instrument]  # internal symbol -> Instrument

    # ---- lookups used by the services ------------------------------------
    @property
    def symbols(self) -> tuple[str, ...]:
        """Every internal symbol, in topic order (fx, crypto, us_stocks)."""
        return tuple(s for t in self.topics.values() for s in t.symbols)

    @property
    def topic_names(self) -> list[str]:
        return [t.name for t in self.topics.values()]

    @property
    def by_ws_symbol(self) -> dict[str, Instrument]:
        """Provider code -> instrument. The normalizer uses this to turn the
        tick's "s" into our internal symbol."""
        return {i.ws_symbol: i for i in self.instruments.values()}

    def instrument(self, symbol: str) -> Instrument:
        return self.instruments[symbol]

    def topic_for(self, symbol: str) -> str:
        """Kafka topic for a symbol, decided by its asset class."""
        return self.topics[self.instruments[symbol].asset_class].name

    def topic_by_name(self, name: str) -> Topic:
        for t in self.topics.values():
            if t.name == name:
                return t
        raise KeyError(name)

    def topics_for(self, symbols) -> list[str]:
        """The topics needed to see these symbols (Risk subscribes to only these)."""
        wanted = {self.topic_for(s) for s in symbols}
        return [n for n in self.topic_names if n in wanted]


# =============================================================================
# Loading and validation
# =============================================================================
def load_topology(path: Path | str = DEFAULT_TOPOLOGY_FILE) -> Topology:
    """Read and validate the topology. Raises TopologyError on any mistake."""
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    return parse_topology(raw)


def parse_topology(raw: dict) -> Topology:
    """Validate an already-parsed TOML document (split out so tests can feed
    dicts without touching the filesystem)."""
    limit = int(raw.get("limits", {}).get("symbol_limit", 0))
    if limit <= 0:
        raise TopologyError("[limits] symbol_limit must be a positive integer")

    instruments: dict[str, Instrument] = {}
    for symbol, spec in raw.get("instruments", {}).items():
        try:
            inst = Instrument(
                symbol=symbol,
                asset_class=spec["asset_class"],
                ws_symbol=spec["ws_symbol"],
                rest_symbol=spec.get("rest_symbol"),
                base_currency=spec.get("base_currency"),
                quote_currency=spec["quote_currency"],
                price_decimals=int(spec["price_decimals"]),
                pip_or_tick_size=float(spec["pip_or_tick_size"]),
            )
        except KeyError as exc:
            raise TopologyError(f"instrument {symbol!r} is missing field {exc}") from None
        if inst.asset_class not in ASSET_CLASSES:
            raise TopologyError(f"instrument {symbol!r}: unknown asset_class {inst.asset_class!r}")
        if inst.pip_or_tick_size <= 0:
            raise TopologyError(f"instrument {symbol!r}: pip_or_tick_size must be > 0")
        instruments[symbol] = inst

    topics: dict[str, Topic] = {}
    seen_names: set[str] = set()
    owner: dict[str, str] = {}  # symbol -> asset class of the topic listing it
    for asset_class in ASSET_CLASSES:  # fixed order: fx, crypto, us_stocks
        spec = raw.get("topics", {}).get(asset_class)
        if spec is None:
            continue
        topic = Topic(
            asset_class=asset_class,
            name=spec["name"],
            partitions=int(spec["partitions"]),
            retention_ms=int(spec["retention_ms"]),
            symbols=tuple(spec["symbols"]),
        )
        if topic.partitions < 1 or topic.retention_ms < 1:
            raise TopologyError(f"topic {topic.name!r}: partitions and retention_ms must be >= 1")
        if topic.name in seen_names:
            raise TopologyError(f"topic name {topic.name!r} is used twice")
        seen_names.add(topic.name)
        for s in topic.symbols:
            if s not in instruments:
                raise TopologyError(
                    f"topic {topic.name!r} lists {s!r}, which has no [instruments] entry"
                )
            if instruments[s].asset_class != asset_class:
                raise TopologyError(
                    f"{s!r} is listed under topics.{asset_class} but its asset_class is "
                    f"{instruments[s].asset_class!r}"
                )
            if s in owner:
                raise TopologyError(f"{s!r} is listed in more than one topic")
            owner[s] = asset_class
        topics[asset_class] = topic
    if extra := sorted(set(instruments) - set(owner)):
        raise TopologyError(f"instruments not listed in any topic: {extra}")
    if not topics:
        raise TopologyError("no [topics.*] defined")

    ws_codes = [i.ws_symbol for i in instruments.values()]
    if dupes := sorted({c for c in ws_codes if ws_codes.count(c) > 1}):
        raise TopologyError(f"ws_symbol used by more than one instrument: {dupes}")
    total = len(owner)
    if total > limit:
        raise TopologyError(
            f"{total} symbols configured but the TraderMade plan allows {limit} per "
            f"session ([limits] symbol_limit); remove symbols or upgrade the plan"
        )
    return Topology(symbol_limit=limit, topics=topics, instruments=instruments)


# =============================================================================
# Kafka's default partitioner, reproduced
#
# The producer sets partitioner=murmur2_random, which hashes keys exactly like
# the Java client: partition = toPositive(murmur2(key)) % num_partitions.
# Reproducing it lets us choose partition counts BEFORE creating topics and
# check the result against the live delivery reports afterwards.
# =============================================================================
def murmur2(data: bytes) -> int:
    """Kafka's murmur2 (org.apache.kafka.common.utils.Utils.murmur2), as an
    unsigned 32-bit int. Masks emulate Java's 32-bit int overflow."""
    length = len(data)
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length - length % 4
    rest = length % 4
    if rest == 3:
        h ^= data[tail + 2] << 16
    if rest >= 2:
        h ^= data[tail + 1] << 8
    if rest >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def partition_for(key: str, num_partitions: int) -> int:
    """The partition Kafka's Java-compatible partitioner picks for `key`.
    toPositive() in Java is `x & 0x7fffffff`."""
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % num_partitions


def expected_partition_map(topology: Topology) -> dict[str, dict[str, int]]:
    """{topic name: {symbol: partition}} for the configured partition counts."""
    return {
        t.name: {s: partition_for(s, t.partitions) for s in t.symbols}
        for t in topology.topics.values()
    }
