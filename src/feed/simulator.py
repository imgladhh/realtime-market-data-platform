import time
import random
import json
import itertools
import logging
from confluent_kafka import Producer
from src.models import MarketEvent, EventType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Starting prices for each symbol
INITIAL_PRICES = {
    "AAPL":  189.00,
    "TSLA":  175.00,
    "GOOGL": 175.00,
    "MSFT":  420.00,
    "BTCUSD": 68000.00,
}

KAFKA_TOPIC = "market-events"


def make_producer(bootstrap_servers: str = "localhost:9092") -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 5,           # small batching
        "compression.type": "lz4",
    })


class FeedSimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = make_producer(bootstrap_servers)
        self.prices = {s: p for s, p in INITIAL_PRICES.items()}
        self._seq = itertools.count(1)   # global sequence counter

    def _next_price(self, symbol: str) -> tuple[float, float]:
        """Random walk: price moves ±0.05%, spread is 0.02%."""
        mid = self.prices[symbol]
        mid *= 1 + random.gauss(0, 0.0005)
        mid = round(mid, 2)
        self.prices[symbol] = mid
        spread = round(mid * 0.0002, 2)
        bid = round(mid - spread / 2, 2)
        ask = round(mid + spread / 2, 2)
        return bid, ask

    def _make_event(self, symbol: str) -> MarketEvent:
        bid, ask = self._next_price(symbol)
        now = MarketEvent.now_ms()
        return MarketEvent(
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_size=random.randint(100, 1000),
            ask_size=random.randint(100, 1000),
            event_ts=now,
            server_ts=now,
            seq=next(self._seq),
            type=EventType.QUOTE,
        )

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")

    def run(self, events_per_second: int = 100):
        """Produce events in round-robin across all symbols."""
        symbols = list(INITIAL_PRICES.keys())
        interval = 1.0 / events_per_second
        logger.info(f"FeedSimulator starting: {len(symbols)} symbols, {events_per_second} events/sec")

        while True:
            for symbol in symbols:
                event = self._make_event(symbol)
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=symbol,                          # partition by symbol
                    value=json.dumps(event.to_dict()),
                    callback=self._delivery_report,
                )
            self.producer.poll(0)                        # trigger delivery callbacks
            time.sleep(interval)


if __name__ == "__main__":
    sim = FeedSimulator()
    sim.run(events_per_second=50)
