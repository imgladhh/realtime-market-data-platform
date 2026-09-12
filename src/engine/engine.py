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

    def _drain_acknowledgements(self, consumer, topic_partition_factory) -> None:
        while True:
            try:
                consumed = self._acknowledgements.get_nowait()
            except queue.Empty:
                return

            try:
                consumer.commit(
                    offsets=[
                        topic_partition_factory(
                            consumed.topic,
                            consumed.partition,
                            consumed.offset + 1,
                        )
                    ],
                    asynchronous=False,
                )
            except Exception as exc:
                # Downstream work already succeeded. A failed commit is safe:
                # Kafka may replay the event and Redis updates are idempotent.
                logger.warning(
                    "Failed to commit Kafka offset %s[%d]@%d: %s",
                    consumed.topic,
                    consumed.partition,
                    consumed.offset,
                    exc,
                )

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

    def __init__(self):
        self.consumer = KafkaConsumerBridge()
        self._processed = 0

    async def process_event(self, event: MarketEvent, snapshot_store: SnapshotStore):
        await snapshot_store.update(event)
        await snapshot_store.publish_event(event)
        self._processed += 1

    async def process_consumed_event(
        self,
        consumed: ConsumedEvent,
        snapshot_store: SnapshotStore,
    ) -> None:
        await self.process_event(consumed.event, snapshot_store)
        self.consumer.acknowledge(consumed)

    async def run(self):
        loop = asyncio.get_running_loop()
        self.consumer.start(loop)
        snapshot_store = SnapshotStore()
        logger.info("DistributionEngine running...")

        try:
            async for consumed in self.consumer.events():
                await self.process_consumed_event(consumed, snapshot_store)

                if self._processed % 500 == 0:
                    logger.info(
                        f"Processed {self._processed} events | "
                        f"last={consumed.event.symbol} seq={consumed.event.seq}"
                    )
        finally:
            self.consumer.stop()
            await snapshot_store.close()

    async def shutdown(self):
        self.consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = DistributionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass
