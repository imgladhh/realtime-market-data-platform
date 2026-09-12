import redis.asyncio as aioredis
import logging
import json
import os
from src.models import MarketEvent, SnapshotData

logger = logging.getLogger(__name__)

SNAPSHOT_TTL = 86400  # 24 hours
EVENT_CHANNEL_PREFIX = "events"


class SnapshotStore:
    def __init__(
        self,
        redis_url: str | None = None,
        namespace: str | None = None,
    ):
        redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        namespace = namespace if namespace is not None else os.getenv(
            "MARKET_DATA_NAMESPACE", ""
        )
        self._prefix = f"{namespace.strip(':')}:" if namespace else ""
        self.redis = aioredis.from_url(redis_url, decode_responses=False)

    def snapshot_key(self, symbol: str) -> str:
        return f"{self._prefix}snapshot:{symbol}"

    def event_channel(self, symbol: str) -> str:
        return f"{self._prefix}{EVENT_CHANNEL_PREFIX}:{symbol}"

    async def update(self, event: MarketEvent) -> None:
        """Atomically update snapshot for a symbol."""
        key = self.snapshot_key(event.symbol)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, mapping={
                "bid":      str(event.bid),
                "ask":      str(event.ask),
                "bid_size": str(event.bid_size),
                "ask_size": str(event.ask_size),
                "seq":      str(event.seq),
                "ts":       str(event.server_ts),
            })
            pipe.expire(key, SNAPSHOT_TTL)
            await pipe.execute()

    async def get(self, symbol: str) -> SnapshotData | None:
        """Fetch current snapshot for a symbol."""
        key = self.snapshot_key(symbol)
        data = await self.redis.hgetall(key)
        if not data:
            return None
        return SnapshotData.from_redis(symbol, data)

    async def publish_event(self, event: MarketEvent) -> int:
        """Publish a market event to Redis Pub/Sub after snapshot update."""
        channel = self.event_channel(event.symbol)
        return await self.redis.publish(channel, json.dumps(event.to_dict()))

    async def ping(self) -> bool:
        return await self.redis.ping()

    async def close(self):
        await self.redis.aclose()
