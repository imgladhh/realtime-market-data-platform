from dataclasses import dataclass, asdict
from enum import Enum
import time


class EventType(str, Enum):
    QUOTE = "quote"
    TRADE = "trade"
    SNAPSHOT = "snapshot"


@dataclass
class MarketEvent:
    symbol: str
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    event_ts: int           # upstream timestamp (ms)
    server_ts: int          # ingestion timestamp (ms)
    seq: int                # global monotonic sequence number
    type: EventType = EventType.QUOTE
    last_price: float = 0.0
    last_size: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["type"] = self.type.value
        return d

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)


@dataclass
class SnapshotData:
    """What gets stored in Redis per symbol."""
    symbol: str
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    seq: int
    ts: int                 # timestamp of last update (ms)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_event(cls, event: MarketEvent) -> "SnapshotData":
        return cls(
            symbol=event.symbol,
            bid=event.bid,
            ask=event.ask,
            bid_size=event.bid_size,
            ask_size=event.ask_size,
            seq=event.seq,
            ts=event.server_ts,
        )

    @classmethod
    def from_redis(cls, symbol: str, data: dict) -> "SnapshotData":
        return cls(
            symbol=symbol,
            bid=float(data[b"bid"]),
            ask=float(data[b"ask"]),
            bid_size=int(data[b"bid_size"]),
            ask_size=int(data[b"ask_size"]),
            seq=int(data[b"seq"]),
            ts=int(data[b"ts"]),
        )
