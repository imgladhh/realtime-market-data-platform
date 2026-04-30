import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from fastapi import WebSocket
from src.models import MarketEvent

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 500        # max pending messages per client
SLOW_CLIENT_DROP_LIMIT = 100  # disconnect if dropped this many messages


class SlowConsumerPolicy(str, Enum):
    DROP_OLDEST = "drop_oldest"   # drop oldest message when queue full
    DISCONNECT  = "disconnect"    # disconnect client when queue full


@dataclass
class ClientStats:
    sent:       int = 0
    dropped:    int = 0
    connected_at: float = field(default_factory=time.time)

    @property
    def uptime_sec(self) -> float:
        return time.time() - self.connected_at


class ClientSession:
    """
    Represents one connected WebSocket client.

    Each client has its own bounded outbound queue and a writer coroutine
    that drains the queue independently. This means a slow client never
    blocks the fanout loop or any other client.
    """

    def __init__(
        self,
        client_id: str,
        websocket: WebSocket,
        policy: SlowConsumerPolicy = SlowConsumerPolicy.DROP_OLDEST,
    ):
        self.client_id   = client_id
        self.websocket   = websocket
        self.policy      = policy
        self.subscriptions: set[str] = set()
        self.stats       = ClientStats()
        self.last_seq: dict[str, int] = {}   # symbol -> last received seq

        # Bounded outbound queue
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self._writer_task: asyncio.Task | None = None
        self._disconnected = asyncio.Event()

    # ── Queue management ─────────────────────────────────────────────────────

    def enqueue(self, message: dict) -> bool:
        """
        Non-blocking enqueue. Returns True if message was queued.

        If queue is full:
          DROP_OLDEST: discard oldest message, enqueue new one
          DISCONNECT:  signal disconnect
        """
        try:
            self._queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            self.stats.dropped += 1

            if self.policy == SlowConsumerPolicy.DROP_OLDEST:
                try:
                    self._queue.get_nowait()   # discard oldest
                    self._queue.put_nowait(message)
                    logger.debug(
                        f"[{self.client_id}] Queue full, dropped oldest "
                        f"(total dropped={self.stats.dropped})"
                    )
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
        Drains the outbound queue and sends to WebSocket.
        Runs as an independent asyncio Task per client.
        """
        try:
            while not self._disconnected.is_set():
                try:
                    message = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

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
                f"sent={self.stats.sent} dropped={self.stats.dropped}"
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
        """
        Returns True if a gap is detected (missed messages).
        Updates last_seq tracking.
        """
        last = self.last_seq.get(symbol)
        self.last_seq[symbol] = seq
        if last is None:
            return False                  # first message, no gap possible
        return seq > last + 5             # tolerance of 5 (allows burst reorder)
