import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from src.engine.snapshot_store import SnapshotStore
from src.models import MarketEvent, SnapshotData, EventType
from tests.conftest import make_event


class TestSnapshotStore:
    """
    Tests for SnapshotStore using a mocked Redis client.
    No real Redis connection needed.
    """

    def make_store(self) -> tuple[SnapshotStore, AsyncMock]:
        """Returns a SnapshotStore with a mocked Redis client."""
        store = SnapshotStore.__new__(SnapshotStore)
        mock_redis = AsyncMock()
        store.redis = mock_redis
        return store, mock_redis

    # ── update ────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_update_calls_hset(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="AAPL", seq=100, bid=189.11, ask=189.13)

        await store.update(event)

        mock_redis.hset.assert_called_once()
        call_kwargs = mock_redis.hset.call_args
        assert call_kwargs[0][0] == "snapshot:AAPL"

    @pytest.mark.asyncio
    async def test_update_writes_correct_fields(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="TSLA", seq=42, bid=175.50, ask=175.54)

        await store.update(event)

        mapping = mock_redis.hset.call_args[1]["mapping"]
        assert mapping["bid"] == "175.5"
        assert mapping["ask"] == "175.54"
        assert mapping["seq"] == "42"

    @pytest.mark.asyncio
    async def test_update_sets_ttl(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="AAPL", seq=1)

        await store.update(event)

        mock_redis.expire.assert_called_once_with("snapshot:AAPL", 86400)

    # ── get ───────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self):
        store, mock_redis = self.make_store()
        mock_redis.hgetall.return_value = {}

        result = await store.get("AAPL")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_snapshot_data(self):
        store, mock_redis = self.make_store()
        mock_redis.hgetall.return_value = {
            b"bid":      b"189.11",
            b"ask":      b"189.13",
            b"bid_size": b"500",
            b"ask_size": b"300",
            b"seq":      b"102938",
            b"ts":       b"1710001234567",
        }

        result = await store.get("AAPL")

        assert isinstance(result, SnapshotData)
        assert result.symbol == "AAPL"
        assert result.bid == 189.11
        assert result.ask == 189.13
        assert result.seq == 102938

    @pytest.mark.asyncio
    async def test_get_uses_correct_key(self):
        store, mock_redis = self.make_store()
        mock_redis.hgetall.return_value = {}

        await store.get("BTCUSD")

        mock_redis.hgetall.assert_called_once_with("snapshot:BTCUSD")

    # ── SnapshotData ──────────────────────────────────────────────────────────

    def test_snapshot_from_event(self):
        event = make_event(symbol="AAPL", seq=999, bid=200.0, ask=200.04)
        snapshot = SnapshotData.from_event(event)

        assert snapshot.symbol == "AAPL"
        assert snapshot.bid == 200.0
        assert snapshot.ask == 200.04
        assert snapshot.seq == 999

    def test_snapshot_to_dict(self):
        event = make_event(symbol="AAPL", seq=1)
        snapshot = SnapshotData.from_event(event)
        d = snapshot.to_dict()

        assert "symbol" in d
        assert "bid" in d
        assert "ask" in d
        assert "seq" in d
        assert "ts" in d

    def test_snapshot_from_redis_parses_bytes(self):
        raw = {
            b"bid":      b"189.11",
            b"ask":      b"189.13",
            b"bid_size": b"500",
            b"ask_size": b"300",
            b"seq":      b"12345",
            b"ts":       b"1710001234567",
        }
        snapshot = SnapshotData.from_redis("AAPL", raw)

        assert snapshot.bid == 189.11
        assert snapshot.seq == 12345
        assert isinstance(snapshot.bid, float)
        assert isinstance(snapshot.seq, int)
