import redis.asyncio as aioredis
import logging
from src.models import MarketEvent, SnapshotData

logger = logging.getLogger(__name__)

SNAPSHOT_TTL = 86400  # 24 hours


class SnapshotStore:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = aioredis.from_url(redis_url, decode_responses=False)

    async def update(self, event: MarketEvent) -> None:
        """Atomically update snapshot for a symbol."""
        key = f"snapshot:{event.symbol}"
        await self.redis.hset(key, mapping={
            "bid":      str(event.bid),
            "ask":      str(event.ask),
            "bid_size": str(event.bid_size),
            "ask_size": str(event.ask_size),
            "seq":      str(event.seq),
            "ts":       str(event.server_ts),
        })
        await self.redis.expire(key, SNAPSHOT_TTL)

    async def get(self, symbol: str) -> SnapshotData | None:
        """Fetch current snapshot for a symbol."""
        key = f"snapshot:{symbol}"
        data = await self.redis.hgetall(key)
        if not data:
            return None
        return SnapshotData.from_redis(symbol, data)

    async def close(self):
        await self.redis.aclose()
