import asyncio

import pytest

from src.gateway import gateway
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from src.gateway.session import ClientSession
from src.models import SnapshotData
from tests.conftest import make_websocket


def make_session() -> ClientSession:
    session = ClientSession(client_id="gateway-test", websocket=make_websocket())
    session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
    return session


@pytest.fixture(autouse=True)
async def reset_gateway_state():
    async with gateway.subscriptions_lock:
        gateway.subscriptions.clear()
        gateway.all_sessions.clear()
    yield
    async with gateway.subscriptions_lock:
        gateway.subscriptions.clear()
        gateway.all_sessions.clear()


class DelayedSnapshotStore:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get(self, symbol):
        self.entered.set()
        await self.release.wait()
        return self.snapshot


@pytest.mark.asyncio
async def test_subscribe_sends_snapshot_and_seeds_seq_before_registering(monkeypatch):
    snapshot = SnapshotData(
        symbol="AAPL",
        bid=189.1,
        ask=189.12,
        bid_size=500,
        ask_size=300,
        seq=42,
        ts=1710001234567,
    )
    store = DelayedSnapshotStore(snapshot)
    monkeypatch.setattr(gateway, "snapshot_store", store)

    session = make_session()
    task = asyncio.create_task(gateway._subscribe(session, "AAPL"))

    await asyncio.wait_for(store.entered.wait(), timeout=0.5)
    async with gateway.subscriptions_lock:
        assert session not in gateway.subscriptions.get("AAPL", set())
    assert "AAPL" not in session.subscriptions

    store.release.set()
    await asyncio.wait_for(task, timeout=0.5)

    queued = session._queue.get_nowait()
    assert queued["type"] == "snapshot"
    assert queued["seq"] == 42
    assert session.last_seq["AAPL"] == 42
    async with gateway.subscriptions_lock:
        assert session in gateway.subscriptions["AAPL"]
    assert "AAPL" in session.subscriptions

