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
    sent:         int = 0
    dropped:      int = 0
    coalesced:    int = 0
    gaps_detected: int = 0          # how many seq gaps were detected
    connected_at: float = field(default_factory=time.time)
    latency:      LatencyTracker = field(default_factory=LatencyTracker)

    @property
    def uptime_sec(self) -> float:
        return time.time() - self.connected_at


class ClientSession:
    """
    Represents one connected WebSocket client.

    Owns:
      - BoundedQueue: outbound message buffer
      - WriterLoop: independent asyncio.Task draining the queue
      - LatencyTracker: p50/p99 dispatch latency
      - last_seq: per-symbol sequence tracking for gap detection
      - aggregator: set by gateway after construction (RAW or AGG_100MS)
    """

    def __init__(
        self,
        client_id: str,
        websocket: WebSocket,
        policy: SlowConsumerPolicy = SlowConsumerPolicy.DROP_OLDEST,
        coalescing: bool = True,
    ):
        self.client_id     = client_id
        self.websocket     = websocket
        self.policy        = policy
        self.coalescing    = coalescing
        self.subscriptions: set[str] = set()
        self.stats         = ClientStats()

        # Per-symbol sequence tracking for gap detection
        # Populated on subscribe (seeded from snapshot seq)
        self.last_seq: dict[str, int] = {}

        # Set by gateway after construction
        self.aggregator = None

        # Coalescing: symbol -> latest pending message not yet dequeued
        self._pending: dict[str, dict] = {}

        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self._writer_task: asyncio.Task | None = None
        self._disconnected = asyncio.Event()

    # ── Gap detection ─────────────────────────────────────────────────────────

    def check_gap(self, symbol: str, seq: int) -> bool:
        """
        Called on every event before enqueuing.

        Returns True if a gap is detected (missed messages).
        A gap means seq jumped by more than 5 from the last seen seq.
        Tolerance of 5 accounts for reordering within a burst.

        When a gap is detected, the caller (client_dispatch_loop in gateway)
        is responsible for re-fetching the snapshot and reseeding last_seq.
        """
        last = self.last_seq.get(symbol)
        if last is None:
            # First message after subscribe — no gap possible,
            # last_seq was already seeded from snapshot in _subscribe()
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
        Non-blocking enqueue with optional coalescing.

        Coalescing: if a message for this symbol is already pending
        in the queue, replace it with the latest one.
        """
        symbol = message.get("symbol")

        if self.coalescing and symbol:
            if symbol in self._pending:
                self._pending[symbol] = message
                self.stats.coalesced += 1
                return True
            self._pending[symbol] = message

        return self._enqueue_raw(message)

    def _enqueue_raw(self, message: dict) -> bool:
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

                # Clear coalescing tracker
                symbol = message.get("symbol")
                if self.coalescing and symbol:
                    self._pending.pop(symbol, None)

                # Record dispatch latency
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
                f"coalesced={self.stats.coalesced} gaps={self.stats.gaps_detected} "
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
            "coalesced":      self.stats.coalesced,
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
