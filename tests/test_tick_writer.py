import asyncio

import pytest

from src.storage.tick_writer import TickWriter
from tests.conftest import make_event


class FakeStore:
    def __init__(self, fail_once=False, timeout=False, schema_fail_once=False):
        self.fail_once = fail_once
        self.timeout = timeout
        self.schema_fail_once = schema_fail_once
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
        if self.timeout:
            raise asyncio.TimeoutError
        if self.fail_once and self.calls == 1:
            raise RuntimeError("db down")
        return len(batch)


class FakeConsumer:
    def __init__(self):
        self.commits = 0

    def commit(self, *args, **kwargs):
        self.commits += 1


@pytest.mark.asyncio
async def test_flush_commits_after_successful_write():
    store = FakeStore()
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [make_event(symbol="AAPL", seq=1)]

    await writer._flush(batch, consumer)

    assert store.batches == [batch]
    assert writer.inserted_ticks == 1
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_flush_retries_without_commit_until_write_succeeds():
    store = FakeStore(fail_once=True)
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [make_event(symbol="AAPL", seq=1)]

    await writer._flush(batch, consumer)

    assert store.calls == 2
    assert writer.inserted_ticks == 1
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_flush_timeout_drops_batch_and_commits_offset():
    store = FakeStore(timeout=True)
    consumer = FakeConsumer()
    writer = TickWriter(store=store)
    batch = [
        make_event(symbol="AAPL", seq=1),
        make_event(symbol="AAPL", seq=2),
    ]

    await writer._flush(batch, consumer)

    assert writer.dropped_ticks == 2
    assert writer.inserted_ticks == 0
    assert consumer.commits == 1


@pytest.mark.asyncio
async def test_ensure_schema_retries_until_success():
    store = FakeStore(schema_fail_once=True)
    writer = TickWriter(store=store)

    await writer._ensure_schema_with_retry()

    assert store.schema_calls == 2
