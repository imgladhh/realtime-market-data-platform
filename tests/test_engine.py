import pytest

from src.engine.engine import DistributionEngine
from tests.conftest import make_event


class FakeSnapshotStore:
    def __init__(self):
        self.updated = []
        self.published = []

    async def update(self, event):
        self.updated.append(event)

    async def publish_event(self, event):
        self.published.append(event)
        return 1


@pytest.mark.asyncio
async def test_distribution_engine_publishes_after_snapshot_update():
    engine = DistributionEngine()
    store = FakeSnapshotStore()
    event = make_event(symbol="AAPL", seq=10)

    await engine.process_event(event, store)

    assert store.updated == [event]
    assert store.published == [event]
    assert engine._processed == 1
