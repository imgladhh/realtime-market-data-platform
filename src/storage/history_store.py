import asyncio
import os
from datetime import UTC, datetime
from typing import Any

from src.models import MarketEvent

DEFAULT_DATABASE_URL = (
    "postgresql://market:market@localhost:5432/market_data"
)
MAX_HISTORY_LIMIT = 10000
DEFAULT_HISTORY_LIMIT = 1000


class HistoryStoreUnavailableError(RuntimeError):
    """The database cannot currently serve history requests."""


def _is_unavailable_error(exc: Exception) -> bool:
    if isinstance(exc, (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError)):
        return True
    return type(exc).__name__ in {
        "CannotConnectNowError",
        "ConnectionDoesNotExistError",
        "ConnectionFailureError",
        "ConnectionRejectionError",
        "TooManyConnectionsError",
    }


class HistoryStore:
    def __init__(
        self,
        database_url: str | None = None,
        command_timeout: float = 5.0,
    ):
        self.database_url = (
            database_url
            or os.getenv("MARKET_DATA_DB_URL")
            or DEFAULT_DATABASE_URL
        )
        self.command_timeout = command_timeout
        self._pool = None

    async def connect(self):
        if self._pool is not None:
            return

        import asyncpg

        try:
            self._pool = await asyncpg.create_pool(
                self.database_url,
                command_timeout=self.command_timeout,
            )
        except Exception as exc:
            if _is_unavailable_error(exc):
                raise HistoryStoreUnavailableError(
                    "history database unavailable"
                ) from exc
            raise

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self):
        await self.connect()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE EXTENSION IF NOT EXISTS timescaledb;

                CREATE TABLE IF NOT EXISTS market_ticks (
                    event_time  TIMESTAMPTZ NOT NULL,
                    symbol      TEXT NOT NULL,
                    seq         BIGINT NOT NULL,
                    bid         DOUBLE PRECISION NOT NULL,
                    ask         DOUBLE PRECISION NOT NULL,
                    bid_size    INTEGER NOT NULL,
                    ask_size    INTEGER NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                SELECT create_hypertable(
                    'market_ticks',
                    'event_time',
                    if_not_exists => TRUE
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    market_ticks_symbol_seq_event_time_idx
                ON market_ticks (symbol, seq, event_time);

                CREATE INDEX IF NOT EXISTS
                    market_ticks_symbol_event_time_idx
                ON market_ticks (symbol, event_time);
                """
            )

    async def insert_ticks(self, events: list[MarketEvent]) -> int:
        if not events:
            return 0

        await self.connect()
        rows = [_event_to_row(event) for event in events]
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                INSERT INTO market_ticks (
                    event_time,
                    symbol,
                    seq,
                    bid,
                    ask,
                    bid_size,
                    ask_size
                )
                SELECT * FROM UNNEST(
                    $1::timestamptz[],
                    $2::text[],
                    $3::bigint[],
                    $4::double precision[],
                    $5::double precision[],
                    $6::integer[],
                    $7::integer[]
                )
                ON CONFLICT (symbol, seq, event_time) DO NOTHING
                """,
                [row["event_time"] for row in rows],
                [row["symbol"] for row in rows],
                [row["seq"] for row in rows],
                [row["bid"] for row in rows],
                [row["ask"] for row in rows],
                [row["bid_size"] for row in rows],
                [row["ask_size"] for row in rows],
            )
        return _rows_affected(result)

    async def fetch_ticks(
        self,
        symbol: str,
        from_ts: int,
        to_ts: int,
        limit: int = DEFAULT_HISTORY_LIMIT,
    ) -> list[dict[str, Any]]:
        await self.connect()
        limit = max(1, min(limit, MAX_HISTORY_LIMIT))
        from_time = _ms_to_datetime(from_ts)
        to_time = _ms_to_datetime(to_ts)

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        seq,
                        (EXTRACT(EPOCH FROM event_time) * 1000)::BIGINT AS event_ts,
                        bid,
                        ask,
                        bid_size,
                        ask_size
                    FROM market_ticks
                    WHERE symbol = $1
                      AND event_time BETWEEN $2 AND $3
                    ORDER BY event_time ASC, seq ASC
                    LIMIT $4
                    """,
                    symbol,
                    from_time,
                    to_time,
                    limit,
                )
        except Exception as exc:
            if _is_unavailable_error(exc):
                raise HistoryStoreUnavailableError(
                    "history database unavailable"
                ) from exc
            raise

        return [
            {
                "seq": int(row["seq"]),
                "event_ts": int(row["event_ts"]),
                "bid": float(row["bid"]),
                "ask": float(row["ask"]),
                "bid_size": int(row["bid_size"]),
                "ask_size": int(row["ask_size"]),
            }
            for row in rows
        ]


def _ms_to_datetime(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)


def _event_to_row(event: MarketEvent) -> dict[str, Any]:
    return {
        "event_time": _ms_to_datetime(event.event_ts),
        "symbol": event.symbol,
        "seq": event.seq,
        "bid": event.bid,
        "ask": event.ask,
        "bid_size": event.bid_size,
        "ask_size": event.ask_size,
    }


def _rows_affected(result: str) -> int:
    try:
        return int(result.rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        return 0
