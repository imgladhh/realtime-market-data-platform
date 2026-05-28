import redis.asyncio as aioredis
import logging
import json
from src.models import MarketEvent, SnapshotData

logger = logging.getLogger(__name__)

SNAPSHOT_TTL = 86400  # 24 hours
EVENT_CHANNEL_PREFIX = "events"


class SnapshotStore:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = aioredis.from_url(redis_url, decode_responses=False)

    async def update(self, event: MarketEvent) -> None:
        """Atomically update snapshot for a symbol."""
        key = f"snapshot:{event.symbol}"
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
        key = f"snapshot:{symbol}"
        data = await self.redis.hgetall(key)
        if not data:
            return None
        return SnapshotData.from_redis(symbol, data)

    async def publish_event(self, event: MarketEvent) -> int:
        """Publish a market event to Redis Pub/Sub after snapshot update."""
        channel = f"{EVENT_CHANNEL_PREFIX}:{event.symbol}"
        return await self.redis.publish(channel, json.dumps(event.to_dict()))

    async def ping(self) -> bool:
        return await self.redis.ping()

    async def close(self):
        await self.redis.aclose()
