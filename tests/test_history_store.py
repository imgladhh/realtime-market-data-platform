import asyncio

import pytest

from src.storage.history_store import (
    HistoryStore,
    MAX_HISTORY_LIMIT,
    _event_to_row,
    _rows_affected,
)
from tests.conftest import make_event


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn
        self.closed = False

    def acquire(self):
        return FakeAcquire(self.conn)

    async def close(self):
        self.closed = True


class FakeConn:
    def __init__(self):
        self.execute_calls = []
        self.fetch_calls = []
        self.fetch_rows = []

    async def execute(self, *args):
        self.execute_calls.append(args)
        return "INSERT 0 1"

    async def fetch(self, *args):
        self.fetch_calls.append(args)
        return self.fetch_rows


@pytest.mark.asyncio
async def test_insert_ticks_uses_bulk_insert_and_deduplicates():
    conn = FakeConn()
    store = HistoryStore()
    store._pool = FakePool(conn)
    event = make_event(symbol="AAPL", seq=100, bid=189.1, ask=189.12)

    inserted = await store.insert_ticks([event])

    assert inserted == 1
    sql = conn.execute_calls[0][0]
    assert "ON CONFLICT (symbol, seq, event_time) DO NOTHING" in sql
    assert conn.execute_calls[0][2] == ["AAPL"]
    assert conn.execute_calls[0][3] == [100]


@pytest.mark.asyncio
async def test_fetch_ticks_returns_serialized_rows_in_query_order():
    conn = FakeConn()
    conn.fetch_rows = [
        {
            "seq": 1,
            "event_ts": 1710001234567,
            "bid": 189.1,
            "ask": 189.12,
            "bid_size": 500,
            "ask_size": 300,
        }
    ]
    store = HistoryStore()
    store._pool = FakePool(conn)

    rows = await store.fetch_ticks("AAPL", 1710001200000, 1710004800000)

    assert rows == [
        {
            "seq": 1,
            "event_ts": 1710001234567,
            "bid": 189.1,
            "ask": 189.12,
            "bid_size": 500,
            "ask_size": 300,
        }
    ]
    sql = conn.fetch_calls[0][0]
    assert "ORDER BY event_time ASC, seq ASC" in sql
    assert conn.fetch_calls[0][1] == "AAPL"


@pytest.mark.asyncio
async def test_fetch_ticks_caps_limit():
    conn = FakeConn()
    store = HistoryStore()
    store._pool = FakePool(conn)

    await store.fetch_ticks("AAPL", 1, 2, limit=MAX_HISTORY_LIMIT + 1)

    assert conn.fetch_calls[0][4] == MAX_HISTORY_LIMIT


def test_event_to_row_derives_event_time_from_event_ts():
    event = make_event(symbol="AAPL", seq=1)

    row = _event_to_row(event)

    assert row["event_time"].timestamp() == pytest.approx(event.event_ts / 1000)
    assert row["symbol"] == "AAPL"
    assert row["seq"] == 1


def test_rows_affected_parses_asyncpg_insert_result():
    assert _rows_affected("INSERT 0 42") == 42
    assert _rows_affected("CREATE TABLE") == 0
