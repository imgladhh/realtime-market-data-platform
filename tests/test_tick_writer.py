import asyncio
import time

import pytest

from src.storage.tick_writer import ConsumedTick, TickWriter
from tests.conftest import make_event


class FakeStore:
    def __init__(
        self,
        fail_once=False,
        timeout_times=0,
        schema_fail_once=False,
        call_order=None,
    ):
        self.fail_once = fail_once
        self.timeout_times = timeout_times
        self.schema_fail_once = schema_fail_once
        self.call_order = call_order
        self.calls = 0
        self.schema_calls = 0
        self.batches = []

    async def ensure_schema(self):
        self.schema_calls += 1
        if self.schema_fail_once and self.schema_calls == 1:
            raise RuntimeError("db down")

    async def insert_ticks(self, batch):
        self.calls += 1
        self.batches.append(batch)
        if self.call_order is not None:
            self.call_order.append("insert")
        if self.timeout_times > 0:
            self.timeout_times -= 1
            raise asyncio.TimeoutError
        if self.fail_once and self.calls == 1:
            raise RuntimeError("db down")
        return len(batch)


class FakeConsumer:
    def __init__(self, call_order=None):
        self.commits = 0
        self.call_order = call_order
        self.commit_args = []

    def commit(self, *args, **kwargs):
        self.commits += 1
        self.commit_args.append((args, kwargs))
        if self.call_order is not None:
            self.call_order.append("commit")


class FakeMessage:
    def __init__(self, topic="market-events", partition=0, offset=0):
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


def make_consumed(seq=1, partition=0, offset=None):
    message = FakeMessage(
        partition=partition,
        offset=seq if offset is None else offset,
    )
    return ConsumedTick(
        event=make_event(symbol="AAPL", seq=seq),
        message=message,
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
    )


@pytest.mark.asyncio
async def test_flush_commits_after_successful_write():
    store = FakeStore()
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [make_consumed(seq=1)]

    await writer._flush(batch, consumer)

    assert store.batches == [[batch[0].event]]
    assert writer.inserted_ticks == 1
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_flush_retries_without_commit_until_write_succeeds():
    store = FakeStore(fail_once=True)
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [make_consumed(seq=1)]

    await writer._flush(batch, consumer)

    assert store.calls == 2
    assert writer.inserted_ticks == 1
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_flush_retries_timeout_without_commit_until_success():
    store = FakeStore(timeout_times=1)
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [make_consumed(seq=1), make_consumed(seq=2)]

    await writer._flush(batch, consumer)

    assert store.calls == 2
    assert writer.dropped_ticks == 0
    assert writer.inserted_ticks == 2
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_flush_shutdown_during_timeout_retry_does_not_commit():
    class StopOnTimeoutStore(FakeStore):
        async def insert_ticks(self, batch):
            self.calls += 1
            writer.stop()
            raise asyncio.TimeoutError

    store = StopOnTimeoutStore()
    consumer = FakeConsumer()
    writer = TickWriter(store=store)

    flushed = await writer._flush([make_consumed(seq=1)], consumer)

    assert flushed is False
    assert consumer.commits == 0
    assert writer.dropped_ticks == 0


@pytest.mark.asyncio
async def test_invalid_message_flushes_valid_batch_before_commit():
    call_order = []
    store = FakeStore(call_order=call_order)
    consumer = FakeConsumer(call_order=call_order)
    writer = TickWriter(store=store)
    batch = [make_consumed(seq=10), make_consumed(seq=11)]
    invalid_message = FakeMessage(offset=12)

    handled = await writer._handle_invalid_message(
        batch,
        invalid_message,
        consumer,
    )

    assert handled is True
    assert store.batches == [[consumed.event for consumed in batch]]
    assert call_order == ["insert", "commit", "commit"]
    assert consumer.commit_args[0][1]["message"] is batch[-1].message
    assert consumer.commit_args[1][1]["message"] is invalid_message


@pytest.mark.asyncio
async def test_invalid_message_does_not_commit_when_valid_batch_unresolved():
    class StopOnTimeoutStore(FakeStore):
        async def insert_ticks(self, batch):
            writer.stop()
            raise asyncio.TimeoutError

    store = StopOnTimeoutStore()
    consumer = FakeConsumer()
    writer = TickWriter(store=store)

    handled = await writer._handle_invalid_message(
        [make_consumed(seq=10), make_consumed(seq=11)],
        FakeMessage(offset=12),
        consumer,
    )

    assert handled is False
    assert consumer.commits == 0


@pytest.mark.asyncio
async def test_invalid_message_with_empty_batch_commits_that_message():
    store = FakeStore()
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    message = object()

    handled = await writer._handle_invalid_message([], message, consumer)

    assert handled is True
    assert consumer.commits == 1
    assert consumer.commit_args[0][1]["message"] is message


def test_commit_batch_uses_last_message_per_partition():
    consumer = FakeConsumer()
    writer = TickWriter(store=FakeStore())
    batch = [
        make_consumed(seq=1, partition=0, offset=10),
        make_consumed(seq=2, partition=1, offset=20),
        make_consumed(seq=3, partition=0, offset=11),
    ]

    writer._commit_batch(batch, consumer)

    committed_messages = {
        call[1]["message"]
        for call in consumer.commit_args
    }
    assert committed_messages == {batch[1].message, batch[2].message}


def test_continuous_partial_batch_flushes_at_latency_deadline(monkeypatch):
    writer = TickWriter(
        store=FakeStore(),
        batch_max_size=500,
        batch_max_latency_ms=100,
    )
    started = 10.0
    monkeypatch.setattr(time, "monotonic", lambda: 10.101)

    assert writer._should_flush([make_consumed(seq=1)], started) is True


def test_full_batch_flushes_before_latency_deadline(monkeypatch):
    writer = TickWriter(store=FakeStore(), batch_max_size=2)
    monkeypatch.setattr(time, "monotonic", lambda: 10.001)

    assert writer._should_flush(
        [make_consumed(seq=1), make_consumed(seq=2)],
        10.0,
    ) is True


def test_poll_timeout_never_exceeds_remaining_batch_deadline(monkeypatch):
    writer = TickWriter(store=FakeStore(), batch_max_latency_ms=20)
    monkeypatch.setattr(time, "monotonic", lambda: 10.015)

    timeout = writer._poll_timeout([make_consumed(seq=1)], 10.0)

    assert timeout == pytest.approx(0.005)


@pytest.mark.asyncio
async def test_ensure_schema_retries_until_success():
    store = FakeStore(schema_fail_once=True)
    writer = TickWriter(store=store)

    await writer._ensure_schema_with_retry()

    assert store.schema_calls == 2
