import asyncio
import threading

import pytest

from src.engine.engine import ConsumedEvent, DistributionEngine, KafkaConsumerBridge
from tests.conftest import make_event


class FakeSnapshotStore:
    def __init__(self, fail_update=False, fail_publish=False):
        self.updated = []
        self.published = []
        self.fail_update = fail_update
        self.fail_publish = fail_publish

    async def update(self, event):
        if self.fail_update:
            raise RuntimeError("snapshot failed")
        self.updated.append(event)

    async def publish_event(self, event):
        if self.fail_publish:
            raise RuntimeError("publish failed")
        self.published.append(event)
        return 1


class FakeBridge:
    def __init__(self):
        self.acknowledged = []

    def acknowledge(self, consumed):
        self.acknowledged.append(consumed)


def consumed_event(seq=10):
    return ConsumedEvent(
        event=make_event(symbol="AAPL", seq=seq),
        topic="market-events",
        partition=2,
        offset=41,
    )


@pytest.mark.asyncio
async def test_distribution_engine_publishes_after_snapshot_update():
    engine = DistributionEngine()
    store = FakeSnapshotStore()
    event = make_event(symbol="AAPL", seq=10)

    await engine.process_event(event, store)

    assert store.updated == [event]
    assert store.published == [event]
    assert engine._processed == 1


@pytest.mark.asyncio
async def test_consumed_event_acknowledged_after_update_and_publish():
    engine = DistributionEngine()
    engine.consumer = FakeBridge()
    store = FakeSnapshotStore()
    consumed = consumed_event()

    await engine.process_consumed_event(consumed, store)

    assert store.updated == [consumed.event]
    assert store.published == [consumed.event]
    assert engine.consumer.acknowledged == [consumed]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["update", "publish"])
async def test_redis_failure_does_not_acknowledge(failure):
    engine = DistributionEngine()
    engine.consumer = FakeBridge()
    store = FakeSnapshotStore(
        fail_update=failure == "update",
        fail_publish=failure == "publish",
    )

    with pytest.raises(RuntimeError):
        await engine.process_consumed_event(consumed_event(), store)

    assert engine.consumer.acknowledged == []


def test_gateway_engine_disables_auto_commit():
    bridge = KafkaConsumerBridge()

    assert bridge._config["enable.auto.commit"] is False


def test_acknowledgement_commits_explicit_next_offset():
    class Consumer:
        def __init__(self):
            self.commits = []

        def commit(self, **kwargs):
            self.commits.append(kwargs)

    bridge = KafkaConsumerBridge()
    consumer = Consumer()
    consumed = consumed_event()
    bridge.acknowledge(consumed)

    bridge._drain_acknowledgements(
        consumer,
        lambda topic, partition, offset: (topic, partition, offset),
    )

    assert consumer.commits == [{
        "offsets": [("market-events", 2, 42)],
        "asynchronous": False,
    }]


@pytest.mark.asyncio
async def test_handoff_waits_for_bounded_queue_capacity():
    bridge = KafkaConsumerBridge()
    bridge._queue = asyncio.Queue(maxsize=1)
    bridge._loop = asyncio.get_running_loop()
    first = consumed_event(seq=1)
    second = consumed_event(seq=2)
    await bridge._queue.put(first)

    handoff = asyncio.get_running_loop().run_in_executor(
        None,
        bridge._handoff,
        second,
    )
    await asyncio.sleep(0.05)
    assert handoff.done() is False

    assert await bridge._queue.get() == first
    assert await asyncio.wait_for(handoff, timeout=0.5) is True
    assert await bridge._queue.get() == second


def test_stop_joins_consumer_thread():
    bridge = KafkaConsumerBridge()
    bridge._thread = threading.Thread(target=bridge._stop_event.wait)
    bridge._thread.start()

    bridge.stop()

    assert bridge._thread.is_alive() is False
