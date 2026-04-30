import asyncio
import json
import logging
import time
import statistics
from dataclasses import dataclass, field
from enum import Enum
from fastapi import WebSocket
from src.models import MarketEvent

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 500
SLOW_CLIENT_DROP_LIMIT = 100


class SlowConsumerPolicy(str, Enum):
    DROP_OLDEST = "drop_oldest"
    DISCONNECT  = "disconnect"


@dataclass
class LatencyTracker:
    """
    Tracks dispatch latency (time from event_ts to send_ts).
    Keeps a rolling window of last 1000 samples.
    """
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
        idx = int(len(sorted_samples) * p / 100)
        idx = min(idx, len(sorted_samples) - 1)
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
    connected_at: float = field(default_factory=time.time)
    latency:      LatencyTracker = field(default_factory=LatencyTracker)

    @property
    def uptime_sec(self) -> float:
        return time.time() - self.connected_at


class ClientSession:
    """
    Represents one connected WebSocket client.

    Phase 3 additions:
      - Coalescing: if queue already has a pending message for the same
        symbol, replace it with the latest one (old data has no value)
      - LatencyTracker: records p50/p99 dispatch latency per client
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
        self.last_seq: dict[str, int] = {}

        # Bounded outbound queue
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

        # Coalescing buffer: symbol -> latest pending message
        # If coalescing is on, we replace stale pending messages
        self._pending: dict[str, dict] = {}
        self._pending_lock = asyncio.Lock()

        self._writer_task: asyncio.Task | None = None
        self._disconnected = asyncio.Event()

    # ── Enqueue with coalescing ───────────────────────────────────────────────

    async def enqueue_async(self, message: dict) -> bool:
        """
        Async enqueue with coalescing support.

        Coalescing logic:
          If there's already a pending message for this symbol in the queue,
          replace it with the latest one. This prevents queue buildup for
          fast-moving symbols when the client is slightly slow.
        """
        symbol = message.get("symbol")

        if self.coalescing and symbol:
            async with self._pending_lock:
                if symbol in self._pending:
                    # Replace stale message with latest
                    self._pending[symbol] = message
                    self.stats.coalesced += 1
                    return True
                else:
                    self._pending[symbol] = message

        return self._enqueue_raw(message)

    def enqueue(self, message: dict) -> bool:
        """Non-async enqueue (used from fanout loop)."""
        symbol = message.get("symbol")

        if self.coalescing and symbol and symbol in self._pending:
            self._pending[symbol] = message
            self.stats.coalesced += 1
            return True

        if self.coalescing and symbol:
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
                        f"[{self.client_id}] Slow consumer threshold reached, disconnecting"
                    )
                    self._disconnected.set()

            return False

    # ── Writer loop ───────────────────────────────────────────────────────────

    async def _writer_loop(self):
        try:
            while not self._disconnected.is_set():
                try:
                    message = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Clear coalescing tracker for this symbol
                symbol = message.get("symbol")
                if self.coalescing and symbol:
                    self._pending.pop(symbol, None)

                # Record latency before sending
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
                f"sent={self.stats.sent} "
                f"dropped={self.stats.dropped} "
                f"coalesced={self.stats.coalesced} "
                f"p50={self.stats.latency.p50}ms "
                f"p99={self.stats.latency.p99}ms"
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

    # ── Seq gap detection ─────────────────────────────────────────────────────

    def check_gap(self, symbol: str, seq: int) -> bool:
        last = self.last_seq.get(symbol)
        self.last_seq[symbol] = seq
        if last is None:
            return False
        return seq > last + 5

    # ── Stats summary ─────────────────────────────────────────────────────────

    def stats_dict(self) -> dict:
        return {
            "sent":          self.stats.sent,
            "dropped":       self.stats.dropped,
            "coalesced":     self.stats.coalesced,
            "uptime_sec":    round(self.stats.uptime_sec, 1),
            "subscriptions": list(self.subscriptions),
            "queue_size":    self._queue.qsize(),
            "latency_ms": {
                "p50":     self.stats.latency.p50,
                "p99":     self.stats.latency.p99,
                "samples": self.stats.latency.sample_count,
            }
        }
