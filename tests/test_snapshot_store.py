import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from src.engine.snapshot_store import SnapshotStore
from src.models import MarketEvent, SnapshotData, EventType
from tests.conftest import make_event


class FakePipeline:
    def __init__(self):
        self.hset_calls = []
        self.expire_calls = []
        self.executed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def hset(self, *args, **kwargs):
        self.hset_calls.append((args, kwargs))
        return self

    def expire(self, *args):
        self.expire_calls.append(args)
        return self

    async def execute(self):
        self.executed = True


class TestSnapshotStore:
    """
    Tests for SnapshotStore using a mocked Redis client.
    No real Redis connection needed.
    """

    def make_store(self) -> tuple[SnapshotStore, AsyncMock]:
        """Returns a SnapshotStore with a mocked Redis client."""
        store = SnapshotStore.__new__(SnapshotStore)
        mock_redis = AsyncMock()
        mock_redis.pipeline = MagicMock(return_value=FakePipeline())
        mock_redis.publish.return_value = 2
        mock_redis.ping.return_value = True
        store.redis = mock_redis
        store._prefix = ""
        return store, mock_redis

    # ── update ────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_update_calls_hset(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="AAPL", seq=100, bid=189.11, ask=189.13)

        await store.update(event)

        pipe = mock_redis.pipeline.return_value
        assert len(pipe.hset_calls) == 1
        call_kwargs = pipe.hset_calls[0]
        assert call_kwargs[0][0] == "snapshot:AAPL"

    @pytest.mark.asyncio
    async def test_update_writes_correct_fields(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="TSLA", seq=42, bid=175.50, ask=175.54)

        await store.update(event)

        pipe = mock_redis.pipeline.return_value
        mapping = pipe.hset_calls[0][1]["mapping"]
        assert mapping["bid"] == "175.5"
        assert mapping["ask"] == "175.54"
        assert mapping["seq"] == "42"

    @pytest.mark.asyncio
    async def test_update_sets_ttl(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="AAPL", seq=1)

        await store.update(event)

        pipe = mock_redis.pipeline.return_value
        assert pipe.expire_calls == [("snapshot:AAPL", 86400)]
        assert pipe.executed is True

    @pytest.mark.asyncio
    async def test_publish_event_uses_symbol_channel(self):
        store, mock_redis = self.make_store()
        event = make_event(symbol="AAPL", seq=10)

        subscribers = await store.publish_event(event)

        assert subscribers == 2
        mock_redis.publish.assert_called_once()
        channel, payload = mock_redis.publish.call_args[0]
        assert channel == "events:AAPL"
        assert json.loads(payload)["seq"] == 10

    @pytest.mark.asyncio
    async def test_namespace_is_applied_to_keys_and_channels(self):
        store, mock_redis = self.make_store()
        store._prefix = "validation-123:"
        event = make_event(symbol="AAPL", seq=10)

        await store.update(event)
        await store.publish_event(event)

        pipe = mock_redis.pipeline.return_value
        assert pipe.hset_calls[0][0][0] == "validation-123:snapshot:AAPL"
        assert mock_redis.publish.call_args[0][0] == "validation-123:events:AAPL"

    @pytest.mark.asyncio
    async def test_ping_delegates_to_redis(self):
        store, mock_redis = self.make_store()

        assert await store.ping() is True
        mock_redis.ping.assert_awaited_once()

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
