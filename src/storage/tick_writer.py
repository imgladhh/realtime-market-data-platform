import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass

from src.models import EventType, MarketEvent
from src.storage.history_store import HistoryStore

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "market-events"
BATCH_MAX_SIZE = 500
BATCH_MAX_LATENCY_MS = 100
MAX_BACKOFF_SECONDS = 5.0


@dataclass(frozen=True)
class ConsumedTick:
    event: MarketEvent
    message: object
    topic: str
    partition: int
    offset: int


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
        batch: list[ConsumedTick] = []
        batch_started = time.monotonic()

        try:
            while not self._stop:
                msg = consumer.poll(
                    timeout=self._poll_timeout(batch, batch_started)
                )
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
                    if not await self._handle_invalid_message(batch, msg, consumer):
                        break
                    batch = []
                    continue

                if not batch:
                    batch_started = time.monotonic()
                batch.append(ConsumedTick(
                    event=event,
                    message=msg,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                ))

                if self._should_flush(batch, batch_started):
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

    def _should_flush(
        self,
        batch: list[ConsumedTick],
        batch_started: float,
    ) -> bool:
        return (
            len(batch) >= self.batch_max_size
            or self._batch_expired(batch_started)
        )

    def _poll_timeout(
        self,
        batch: list[ConsumedTick],
        batch_started: float,
    ) -> float:
        if not batch:
            return 0.05
        elapsed = time.monotonic() - batch_started
        remaining = max(self.batch_max_latency_ms / 1000.0 - elapsed, 0.0)
        return min(0.05, remaining)

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

    async def _handle_invalid_message(self, batch, message, consumer) -> bool:
        if batch:
            # Persist and explicitly commit earlier valid records before
            # advancing this partition past the malformed message.
            if not await self._flush(batch, consumer):
                return False

        consumer.commit(message=message, asynchronous=False)
        return True

    @staticmethod
    def _commit_batch(batch: list[ConsumedTick], consumer) -> None:
        last_by_partition: dict[tuple[str, int], ConsumedTick] = {}
        for consumed in batch:
            key = (consumed.topic, consumed.partition)
            previous = last_by_partition.get(key)
            if previous is None or consumed.offset > previous.offset:
                last_by_partition[key] = consumed

        for consumed in last_by_partition.values():
            consumer.commit(message=consumed.message, asynchronous=False)

    async def _flush(self, batch: list[ConsumedTick], consumer) -> bool:
        backoff = 0.1
        while not self._stop:
            try:
                inserted = await self.store.insert_ticks(
                    [consumed.event for consumed in batch]
                )
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning(
                    "Timed out writing %d ticks; retrying in %.1fs",
                    len(batch),
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue
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
            self._commit_batch(batch, consumer)
            return True

        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    writer = TickWriter(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    asyncio.run(writer.run())
