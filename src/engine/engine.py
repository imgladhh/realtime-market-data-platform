import asyncio
import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError
from src.models import MarketEvent, EventType
from src.engine.snapshot_store import SnapshotStore

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "market-events"


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
            "enable.auto.commit": True,
        }
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10000)
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
                server_ts=int(d["server_ts"]),
                seq=int(d["seq"]),
                type=EventType(d.get("type", "quote")),
            )
        except Exception as e:
            logger.error(f"Failed to parse event: {e} | raw={raw}")
            return None

    def _consume_loop(self):
        consumer = Consumer(self._config)
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Kafka consumer started, topic={KAFKA_TOPIC}")

        while not self._stop_event.is_set():
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            event = self._parse_event(msg.value())
            if event and self._loop:
                # Thread-safe bridge into asyncio event loop
                asyncio.run_coroutine_threadsafe(
                    self._queue.put(event), self._loop
                )

        consumer.close()
        logger.info("Kafka consumer stopped")

    def start(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="kafka-consumer"
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    async def events(self):
        """Async generator: yields MarketEvents as they arrive."""
        while True:
            event = await self._queue.get()
            yield event


class DistributionEngine:
    """
    Consumes events from Kafka and updates Redis snapshots.
    Fanout to WebSocket clients will be added in the next step.
    """

    def __init__(self):
        self.consumer = KafkaConsumerBridge()
        self.snapshot_store = SnapshotStore()
        self._processed = 0

    async def run(self):
        loop = asyncio.get_running_loop()
        self.consumer.start(loop)
        logger.info("DistributionEngine running...")

        async for event in self.consumer.events():
            await self.snapshot_store.update(event)
            self._processed += 1

            if self._processed % 500 == 0:
                logger.info(
                    f"Processed {self._processed} events | "
                    f"last={event.symbol} seq={event.seq}"
                )

    async def shutdown(self):
        self.consumer.stop()
        await self.snapshot_store.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = DistributionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass
