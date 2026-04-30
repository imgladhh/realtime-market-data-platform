import asyncio
import logging
import time
from enum import Enum
from src.models import MarketEvent

logger = logging.getLogger(__name__)


class AggregationMode(str, Enum):
    RAW      = "raw"       # every tick, lowest latency
    AGG_100MS = "agg_100ms"  # coalesce into 100ms windows


class AggregationBuffer:
    """
    Buffers incoming events per symbol and flushes on a fixed interval.

    In RAW mode: events pass through immediately.
    In AGG_100MS mode: within each 100ms window, only the latest
    event per symbol is emitted. This trades latency for bandwidth.

    Real-world analogy: retail clients get 100ms aggregated,
    professional clients get raw tick.
    """

    def __init__(
        self,
        mode: AggregationMode = AggregationMode.RAW,
        interval_ms: int = 100,
    ):
        self.mode        = mode
        self.interval_ms = interval_ms
        self._buffer: dict[str, MarketEvent] = {}  # symbol -> latest event
        self._output: asyncio.Queue[MarketEvent] = asyncio.Queue()
        self._flush_task: asyncio.Task | None = None

    async def push(self, event: MarketEvent):
        if self.mode == AggregationMode.RAW:
            await self._output.put(event)
        else:
            # Buffer: latest event per symbol wins
            self._buffer[event.symbol] = event

    async def _flush_loop(self):
        """Periodically flush buffered events to output queue."""
        interval_sec = self.interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval_sec)
            if self._buffer:
                for event in self._buffer.values():
                    await self._output.put(event)
                flushed = len(self._buffer)
                self._buffer.clear()
                logger.debug(f"Aggregator flushed {flushed} events")

    def start(self):
        if self.mode != AggregationMode.RAW:
            self._flush_task = asyncio.create_task(
                self._flush_loop(), name="aggregator-flush"
            )

    def stop(self):
        if self._flush_task:
            self._flush_task.cancel()

    async def events(self):
        """Async generator: yields events after aggregation."""
        while True:
            event = await self._output.get()
            yield event
