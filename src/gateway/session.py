import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from fastapi import WebSocket

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 500
SLOW_CLIENT_DROP_LIMIT = 100


class SlowConsumerPolicy(str, Enum):
    DROP_OLDEST = "drop_oldest"
    DISCONNECT  = "disconnect"


@dataclass
class LatencyTracker:
    """Rolling window of last 1000 dispatch latency samples."""
    _samples: list[float] = field(default_factory=list)
    _max_samples: int = 1000

    def record(self, event_ts_ms: int):
        latency_ms = (time.time() * 1000) - event_ts_ms
        self._samples.append(latency_ms)
        if len(self._samples) > self._max_samples:
            self._samples.pop(0)

    def percentile(self, p: float) -> float:
        if not self._samples:
            return 0.0
        sorted_samples = sorted(self._samples)
        idx = min(int(len(sorted_samples) * p / 100), len(sorted_samples) - 1)
        return round(sorted_samples[idx], 2)

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    @property
    def sample_count(self) -> int:
        return len(self._samples)


@dataclass
class ClientStats:
    sent:          int = 0
    dropped:       int = 0
    gaps_detected: int = 0
    connected_at:  float = field(default_factory=time.time)
    latency:       LatencyTracker = field(default_factory=LatencyTracker)

    @property
    def uptime_sec(self) -> float:
        return time.time() - self.connected_at


class ClientSession:
    """
    Represents one connected WebSocket client.

    Owns:
      - BoundedQueue: outbound message buffer (maxsize=500)
      - WriterLoop: independent asyncio.Task draining the queue
      - LatencyTracker: rolling p50/p99 dispatch latency
      - last_seq: per-symbol sequence tracking for gap detection
      - aggregator: per-client AggregationBuffer (RAW or AGG_100MS)
                    set by gateway after construction

    Aggregation / coalescing is handled entirely by AggregationBuffer:
      - RAW mode:      every tick delivered in order, gap detection enabled
      - AGG_100MS mode: latest-per-symbol per 100ms window, gap detection disabled

    ClientSession itself does NOT do coalescing — keeping message ordering
    correct while coalescing in a single asyncio.Queue requires replacing
    queue entries in-place, which asyncio.Queue does not support.
    The AggregationBuffer layer handles this cleanly before enqueue.
    """

    def __init__(
        self,
        client_id: str,
        websocket: WebSocket,
        policy: SlowConsumerPolicy = SlowConsumerPolicy.DROP_OLDEST,
    ):
        self.client_id     = client_id
        self.websocket     = websocket
        self.policy        = policy
        self.subscriptions: set[str] = set()
        self.stats         = ClientStats()
        self.last_seq:     dict[str, int] = {}
        self.aggregator    = None  # set by gateway after construction

        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self._writer_task: asyncio.Task | None = None
        self._disconnected = asyncio.Event()

    # ── Gap detection ─────────────────────────────────────────────────────────

    def check_gap(self, symbol: str, seq: int) -> bool:
        """
        Returns True if a seq gap is detected (missed messages).
        Tolerance of 5 accounts for minor reordering within a burst.

        Only called in RAW mode. In AGG_100MS mode, seq jumps are
        expected (intermediate events are intentionally skipped).
        """
        last = self.last_seq.get(symbol)
        if last is None:
            self.last_seq[symbol] = seq
            return False

        gap = seq > last + 5
        if gap:
            self.stats.gaps_detected += 1
            logger.warning(
                f"[{self.client_id}] Seq gap on {symbol}: "
                f"last={last} current={seq} delta={seq - last}"
            )
        self.last_seq[symbol] = seq
        return gap

    # ── Enqueue ───────────────────────────────────────────────────────────────

    def enqueue(self, message: dict) -> bool:
        """
        Non-blocking enqueue into the bounded outbound queue.

        If queue is full, SlowConsumerPolicy determines behavior:
          DROP_OLDEST: discard oldest message, enqueue latest
          DISCONNECT:  signal disconnect after threshold
        """
        try:
            self._queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            self.stats.dropped += 1

            if self.policy == SlowConsumerPolicy.DROP_OLDEST:
                try:
                    self._queue.get_nowait()
                    self._queue.put_nowait(message)
                    return True
                except asyncio.QueueEmpty:
                    pass

            elif self.policy == SlowConsumerPolicy.DISCONNECT:
                if self.stats.dropped >= SLOW_CLIENT_DROP_LIMIT:
                    logger.warning(
                        f"[{self.client_id}] Slow consumer threshold reached "
                        f"(dropped={self.stats.dropped}), disconnecting"
                    )
                    self._disconnected.set()

            return False

    # ── Writer loop ───────────────────────────────────────────────────────────

    async def _writer_loop(self):
        """
        Independent asyncio.Task per client.
        Drains the outbound queue and writes to WebSocket.
        Slow clients never block other clients.
        """
        try:
            while not self._disconnected.is_set():
                try:
                    message = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                event_ts = message.get("event_ts")
                if event_ts:
                    self.stats.latency.record(event_ts)

                try:
                    await self.websocket.send_text(json.dumps(message))
                    self.stats.sent += 1
                except Exception as e:
                    logger.info(f"[{self.client_id}] Send failed: {e}")
                    self._disconnected.set()
                    break

        except asyncio.CancelledError:
            pass
        finally:
            logger.info(
                f"[{self.client_id}] Writer stopped | "
                f"sent={self.stats.sent} dropped={self.stats.dropped} "
                f"gaps={self.stats.gaps_detected} "
                f"p50={self.stats.latency.p50}ms p99={self.stats.latency.p99}ms"
            )

    def start_writer(self):
        self._writer_task = asyncio.create_task(
            self._writer_loop(), name=f"writer-{self.client_id}"
        )

    async def wait_until_disconnected(self):
        await self._disconnected.wait()

    async def close(self):
        self._disconnected.set()
        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
        try:
            await self.websocket.close()
        except Exception:
            pass

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats_dict(self) -> dict:
        return {
            "sent":           self.stats.sent,
            "dropped":        self.stats.dropped,
            "gaps_detected":  self.stats.gaps_detected,
            "uptime_sec":     round(self.stats.uptime_sec, 1),
            "subscriptions":  list(self.subscriptions),
            "queue_size":     self._queue.qsize(),
            "aggregation_mode": self.aggregator.mode.value if self.aggregator else "unknown",
            "latency_ms": {
                "p50":     self.stats.latency.p50,
                "p99":     self.stats.latency.p99,
                "samples": self.stats.latency.sample_count,
            }
        }
