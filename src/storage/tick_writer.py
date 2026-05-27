import asyncio
import json
import logging
import os
import time

from src.models import EventType, MarketEvent
from src.storage.history_store import HistoryStore

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "market-events"
BATCH_MAX_SIZE = 500
BATCH_MAX_LATENCY_MS = 100
MAX_BACKOFF_SECONDS = 5.0


class TickWriter:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "tick-storage",
        store: HistoryStore | None = None,
        batch_max_size: int = BATCH_MAX_SIZE,
        batch_max_latency_ms: int = BATCH_MAX_LATENCY_MS,
    ):
        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.store = store or HistoryStore()
        self.batch_max_size = batch_max_size
        self.batch_max_latency_ms = batch_max_latency_ms
        self.dropped_ticks = 0
        self.inserted_ticks = 0
        self._stop = False

    def stop(self):
        self._stop = True

    def _parse_event(self, raw: bytes) -> MarketEvent | None:
        try:
            data = json.loads(raw)
            return MarketEvent(
                symbol=data["symbol"],
                bid=float(data["bid"]),
                ask=float(data["ask"]),
                bid_size=int(data["bid_size"]),
                ask_size=int(data["ask_size"]),
                event_ts=int(data["event_ts"]),
                server_ts=int(data["server_ts"]),
                seq=int(data["seq"]),
                type=EventType(data.get("type", "quote")),
            )
        except Exception as exc:
            logger.error("Failed to parse tick event: %s | raw=%r", exc, raw)
            return None

    async def run(self):
        from confluent_kafka import Consumer, KafkaError

        await self._ensure_schema_with_retry()
        consumer = Consumer(self._config)
        consumer.subscribe([KAFKA_TOPIC])
        batch: list[MarketEvent] = []
        batch_started = time.monotonic()

        try:
            while not self._stop:
                msg = consumer.poll(timeout=0.05)
                if msg is None:
                    if batch and self._batch_expired(batch_started):
                        await self._flush(batch, consumer)
                        batch = []
                    await asyncio.sleep(0)
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                event = self._parse_event(msg.value())
                if event is None:
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                if not batch:
                    batch_started = time.monotonic()
                batch.append(event)

                if len(batch) >= self.batch_max_size:
                    await self._flush(batch, consumer)
                    batch = []

            if batch:
                await self._flush(batch, consumer)
        finally:
            consumer.close()
            await self.store.close()

    def _batch_expired(self, batch_started: float) -> bool:
        elapsed_ms = (time.monotonic() - batch_started) * 1000
        return elapsed_ms >= self.batch_max_latency_ms

    async def _ensure_schema_with_retry(self):
        backoff = 0.1
        while not self._stop:
            try:
                await self.store.ensure_schema()
                return
            except Exception as exc:
                logger.warning(
                    "Failed to initialize tick storage schema: %s; retrying in %.1fs",
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    async def _flush(self, batch: list[MarketEvent], consumer):
        backoff = 0.1
        while not self._stop:
            try:
                inserted = await self.store.insert_ticks(batch)
            except (TimeoutError, asyncio.TimeoutError):
                self.dropped_ticks += len(batch)
                logger.error("Timed out writing %d ticks; dropping batch", len(batch))
                consumer.commit(asynchronous=False)
                return
            except Exception as exc:
                logger.warning(
                    "Failed to write %d ticks: %s; retrying in %.1fs",
                    len(batch),
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue

            self.inserted_ticks += inserted
            consumer.commit(asynchronous=False)
            return


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    writer = TickWriter(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    asyncio.run(writer.run())
