import asyncio
import concurrent.futures
import json
import logging
import queue
import threading
from dataclasses import dataclass
from src.models import MarketEvent, EventType
from src.engine.snapshot_store import SnapshotStore

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "market-events"
REDIS_RETRY_INITIAL_SECONDS = 0.1
REDIS_RETRY_MAX_SECONDS = 5.0


@dataclass(frozen=True)
class ConsumedEvent:
    event: MarketEvent
    topic: str
    partition: int
    offset: int


class KafkaConsumerBridge:
    """
    Runs Kafka consumer in a background thread (confluent_kafka is blocking),
    bridges events into the asyncio event loop via asyncio.Queue.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "gateway-engine",
    ):
        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
        self._queue: asyncio.Queue[ConsumedEvent] = asyncio.Queue(maxsize=10000)
        self._acknowledgements: queue.SimpleQueue[ConsumedEvent] = queue.SimpleQueue()
        self._pending_acknowledgements: dict[
            tuple[str, int], dict[int, ConsumedEvent]
        ] = {}
        self._next_commit_offsets: dict[tuple[str, int], int] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def _parse_event(self, raw: bytes) -> MarketEvent | None:
        try:
            d = json.loads(raw)
            return MarketEvent(
                symbol=d["symbol"],
                bid=float(d["bid"]),
                ask=float(d["ask"]),
                bid_size=int(d["bid_size"]),
                ask_size=int(d["ask_size"]),
                event_ts=int(d["event_ts"]),
                server_ts=MarketEvent.now_ms(),
                seq=int(d["seq"]),
                type=EventType(d.get("type", "quote")),
            )
        except Exception as e:
            logger.error(f"Failed to parse event: {e} | raw={raw}")
            return None

    def _handoff(self, consumed: ConsumedEvent) -> bool:
        if self._loop is None:
            return False

        future = asyncio.run_coroutine_threadsafe(
            self._queue.put(consumed),
            self._loop,
        )
        while not self._stop_event.is_set():
            try:
                future.result(timeout=0.1)
                return True
            except concurrent.futures.TimeoutError:
                continue
            except (concurrent.futures.CancelledError, RuntimeError) as exc:
                logger.warning("Kafka handoff failed: %s", exc)
                return False

        future.cancel()
        return False

    def acknowledge(self, consumed: ConsumedEvent) -> None:
        self._acknowledgements.put(consumed)

    def _track_consumed(self, consumed: ConsumedEvent) -> None:
        key = (consumed.topic, consumed.partition)
        self._next_commit_offsets.setdefault(key, consumed.offset)

    def _drain_acknowledgements(self, consumer, topic_partition_factory) -> None:
        while True:
            try:
                consumed = self._acknowledgements.get_nowait()
            except queue.Empty:
                break
            key = (consumed.topic, consumed.partition)
            self._pending_acknowledgements.setdefault(key, {})[
                consumed.offset
            ] = consumed

        commit_offsets = []
        advances: dict[tuple[str, int], tuple[int, int]] = {}
        for key, pending in self._pending_acknowledgements.items():
            start = self._next_commit_offsets.get(key)
            if start is None:
                continue
            cursor = start
            while cursor in pending:
                cursor += 1
            if cursor > start:
                commit_offsets.append(topic_partition_factory(key[0], key[1], cursor))
                advances[key] = (start, cursor)

        if not commit_offsets:
            return

        try:
            consumer.commit(offsets=commit_offsets, asynchronous=False)
        except Exception as exc:
            # Downstream work already succeeded. A failed commit is safe:
            # Kafka may replay the event and Redis updates are idempotent.
            logger.warning("Failed to commit Kafka acknowledgements: %s", exc)
            return

        for key, (start, cursor) in advances.items():
            pending = self._pending_acknowledgements[key]
            for offset in range(start, cursor):
                pending.pop(offset, None)
            self._next_commit_offsets[key] = cursor

    def _consume_loop(self):
        from confluent_kafka import Consumer, KafkaError, TopicPartition

        consumer = Consumer(self._config)
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Kafka consumer started, topic={KAFKA_TOPIC}")

        try:
            while not self._stop_event.is_set():
                self._drain_acknowledgements(consumer, TopicPartition)
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                event = self._parse_event(msg.value())
                if event is None:
                    continue

                consumed = ConsumedEvent(
                    event=event,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                self._track_consumed(consumed)
                if not self._handoff(consumed):
                    break
        finally:
            self._drain_acknowledgements(consumer, TopicPartition)
            consumer.close()
            logger.info("Kafka consumer stopped")

    def start(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="kafka-consumer"
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)

    async def events(self):
        """Async generator: yields Kafka events and acknowledgement metadata."""
        while True:
            consumed = await self._queue.get()
            yield consumed


class DistributionEngine:
    """
    Consumes events from Kafka and updates Redis snapshots.
    Fanout to WebSocket clients will be added in the next step.
    """

    def __init__(
        self,
        redis_retry_initial: float = REDIS_RETRY_INITIAL_SECONDS,
        redis_retry_max: float = REDIS_RETRY_MAX_SECONDS,
    ):
        self.consumer = KafkaConsumerBridge()
        self._processed = 0
        self._redis_retries = 0
        self._shutdown = asyncio.Event()
        self._redis_retry_initial = redis_retry_initial
        self._redis_retry_max = redis_retry_max

    async def process_event(self, event: MarketEvent, snapshot_store: SnapshotStore):
        await snapshot_store.update(event)
        await snapshot_store.publish_event(event)
        self._processed += 1

    async def process_consumed_event(
        self,
        consumed: ConsumedEvent,
        snapshot_store: SnapshotStore,
    ) -> bool:
        backoff = self._redis_retry_initial
        while not self._shutdown.is_set():
            try:
                # Retrying may repeat update and publish after an ambiguous
                # failure. Snapshot writes are idempotent; replayed Pub/Sub
                # events are rejected downstream by per-symbol sequence state.
                await self.process_event(consumed.event, snapshot_store)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._redis_retries += 1
                logger.warning(
                    "Redis processing failed for %s[%d]@%d: %s; "
                    "retrying in %.1fs",
                    consumed.topic,
                    consumed.partition,
                    consumed.offset,
                    exc,
                    backoff,
                )
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, self._redis_retry_max)
                continue

            self.consumer.acknowledge(consumed)
            return True

        return False

    async def run(self):
        loop = asyncio.get_running_loop()
        self.consumer.start(loop)
        snapshot_store = SnapshotStore()
        logger.info("DistributionEngine running...")

        try:
            async for consumed in self.consumer.events():
                if not await self.process_consumed_event(consumed, snapshot_store):
                    break

                if self._processed % 500 == 0:
                    logger.info(
                        f"Processed {self._processed} events | "
                        f"last={consumed.event.symbol} seq={consumed.event.seq}"
                    )
        finally:
            self.consumer.stop()
            await snapshot_store.close()

    async def shutdown(self):
        self._shutdown.set()
        self.consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = DistributionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass
