import asyncio
import json
import time

import pytest

from src.gateway import gateway
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from src.gateway.session import ClientSession, Encoding
from src.models import SnapshotData
from tests.conftest import make_event, make_websocket


def make_session(client_id: str = "gateway-test") -> ClientSession:
    session = ClientSession(client_id=client_id, websocket=make_websocket())
    session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
    return session


def response_text(response) -> str:
    return response.body.decode()


def response_json(response):
    return json.loads(response.body.decode())


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


@pytest.mark.asyncio
async def test_subscribe_stores_filter_and_seeds_last_price(monkeypatch):
    snapshot = SnapshotData(
        symbol="AAPL",
        bid=189.1,
        ask=189.12,
        bid_size=500,
        ask_size=300,
        seq=42,
        ts=1710001234567,
    )

    class Store:
        async def get(self, symbol):
            return snapshot

    monkeypatch.setattr(gateway, "snapshot_store", Store())
    session = make_session()

    await gateway._subscribe(
        session,
        "AAPL",
        {"min_change_pct": 0.05, "max_spread": 0.50},
    )

    assert session.filters["AAPL"].min_change_pct == 0.05
    assert session.filters["AAPL"].max_spread == 0.50
    assert session.last_price["AAPL"] == 189.1


@pytest.mark.asyncio
async def test_unsubscribe_clears_filter_and_last_price():
    session = make_session()
    session.subscriptions.add("AAPL")
    session.filters["AAPL"] = gateway.SubscriptionFilter(min_change_pct=0.05)
    session.last_price["AAPL"] = 189.1
    async with gateway.subscriptions_lock:
        gateway.subscriptions["AAPL"] = {session}

    await gateway._unsubscribe(session, "AAPL")

    assert "AAPL" not in session.filters
    assert "AAPL" not in session.last_price
    assert "AAPL" not in session.subscriptions


@pytest.mark.asyncio
async def test_client_dispatch_filters_incrementals():
    session = make_session()
    session.filters["AAPL"] = gateway.SubscriptionFilter(min_change_pct=0.05)
    session.last_price["AAPL"] = 100.0
    session.aggregator.start()

    task = asyncio.create_task(gateway.client_dispatch_loop(session))
    session.aggregator.push(make_event(symbol="AAPL", seq=101, bid=100.04))
    session.aggregator.push(make_event(symbol="AAPL", seq=102, bid=100.05))

    delivered = await asyncio.wait_for(session._queue.get(), timeout=0.5)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert delivered["seq"] == 102
    assert session.last_price["AAPL"] == 100.05


@pytest.mark.asyncio
async def test_gap_recovery_resets_last_price(monkeypatch):
    snapshot = SnapshotData(
        symbol="AAPL",
        bid=200.0,
        ask=200.1,
        bid_size=500,
        ask_size=300,
        seq=200,
        ts=1710001234567,
    )

    class Store:
        async def get(self, symbol):
            return snapshot

    monkeypatch.setattr(gateway, "snapshot_store", Store())
    session = make_session()
    session.last_seq["AAPL"] = 100
    session.last_price["AAPL"] = 100.0
    session.aggregator.start()

    task = asyncio.create_task(gateway.client_dispatch_loop(session))
    session.aggregator.push(make_event(symbol="AAPL", seq=200))

    delivered = await asyncio.wait_for(session._queue.get(), timeout=0.5)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert delivered["type"] == "snapshot"
    assert delivered["seq"] == 200
    assert session.last_price["AAPL"] == 200.0


def test_encoding_param_msgpack():
    assert gateway._parse_encoding("gateway-test", "msgpack") == Encoding.MSGPACK


def test_encoding_param_default_is_json():
    assert gateway._parse_encoding("gateway-test", None) == Encoding.JSON


def test_encoding_param_invalid_falls_back_to_json(caplog):
    with caplog.at_level("WARNING"):
        assert gateway._parse_encoding("gateway-test", "protobuf") == Encoding.JSON

    assert "Unknown encoding 'protobuf'" in caplog.text


def test_invalid_subscription_filter_is_ignored(caplog):
    with caplog.at_level("WARNING"):
        result = gateway._parse_subscription_filter({"min_change_pct": "nope"})

    assert result is None
    assert "Invalid subscription filter" in caplog.text


@pytest.mark.asyncio
async def test_prometheus_content_type():
    response = await gateway.get_metrics_prometheus()

    assert response.headers["content-type"] == "text/plain; version=0.0.4"


@pytest.mark.asyncio
async def test_prometheus_system_metrics_present():
    response = await gateway.get_metrics_prometheus()
    text = response_text(response)

    assert "# HELP market_data_connected_clients" in text
    assert "# TYPE market_data_connected_clients gauge" in text
    assert "market_data_connected_clients 0" in text
    assert "# HELP market_data_events_dispatched_total" in text
    assert "# TYPE market_data_events_dispatched_total counter" in text


@pytest.mark.asyncio
async def test_prometheus_no_client_labels_when_empty():
    response = await gateway.get_metrics_prometheus()

    assert "client_id=" not in response_text(response)


@pytest.mark.asyncio
async def test_prometheus_per_client_metrics():
    session = make_session(client_id="abc")
    session.stats.sent = 100
    session.stats.dropped = 2
    session.stats.latency.record(int(time.time() * 1000) - 10)

    async with gateway.subscriptions_lock:
        gateway.all_sessions["abc"] = session

    response = await gateway.get_metrics_prometheus()
    text = response_text(response)

    assert 'market_data_client_sent_total{client_id="abc"} 100' in text
    assert 'market_data_client_dropped_total{client_id="abc"} 2' in text
    assert 'market_data_client_latency_p50_ms{client_id="abc"}' in text
    assert 'market_data_client_latency_p99_ms{client_id="abc"}' in text


@pytest.mark.asyncio
async def test_prometheus_latency_omitted_when_no_samples():
    async with gateway.subscriptions_lock:
        gateway.all_sessions["abc"] = make_session(client_id="abc")

    response = await gateway.get_metrics_prometheus()
    text = response_text(response)

    assert "market_data_client_latency_p50_ms" not in text
    assert "market_data_client_latency_p99_ms" not in text


@pytest.mark.asyncio
async def test_prometheus_help_type_lines_format():
    response = await gateway.get_metrics_prometheus()
    lines = response_text(response).splitlines()

    help_idx = lines.index(
        "# HELP market_data_connected_clients Current number of connected WebSocket clients"
    )
    type_idx = lines.index("# TYPE market_data_connected_clients gauge")
    assert help_idx < type_idx

    assert "# TYPE market_data_events_dispatched_total counter" in lines
    assert "# TYPE market_data_throughput_events_per_sec gauge" in lines


class FakeHistoryStore:
    def __init__(self, ticks=None, exc=None):
        self.ticks = ticks or []
        self.exc = exc
        self.calls = []

    async def fetch_ticks(self, **kwargs):
        self.calls.append(kwargs)
        if self.exc:
            raise self.exc
        return self.ticks


@pytest.mark.asyncio
async def test_history_range_query_returns_ticks(monkeypatch):
    store = FakeHistoryStore(ticks=[
        {
            "seq": 101,
            "event_ts": 1710001234567,
            "bid": 189.10,
            "ask": 189.12,
            "bid_size": 500,
            "ask_size": 300,
        }
    ])
    monkeypatch.setattr(gateway, "history_store", store)

    response = await gateway.get_history(
        "aapl",
        from_ts="1710001200000",
        to_ts="1710004800000",
    )

    assert response["symbol"] == "AAPL"
    assert response["count"] == 1
    assert response["ticks"][0]["seq"] == 101
    assert store.calls[0]["symbol"] == "AAPL"
    assert store.calls[0]["from_ts"] == 1710001200000


@pytest.mark.asyncio
async def test_history_from_after_to_returns_400():
    response = await gateway.get_history("AAPL", from_ts="2", to_ts="1")

    assert response.status_code == 400
    assert response_json(response)["error"] == "from_ts must be <= to_ts"


@pytest.mark.asyncio
async def test_history_missing_params_returns_400():
    response = await gateway.get_history("AAPL", from_ts=None, to_ts="1")

    assert response.status_code == 400
    assert response_json(response)["error"] == "from_ts and to_ts required"


@pytest.mark.asyncio
async def test_history_invalid_params_returns_400():
    response = await gateway.get_history("AAPL", from_ts="nope", to_ts="1")

    assert response.status_code == 400
    assert response_json(response)["error"] == "from_ts, to_ts, and limit must be integers"


@pytest.mark.asyncio
async def test_history_non_positive_limit_returns_400():
    response = await gateway.get_history("AAPL", from_ts="1", to_ts="2", limit="0")

    assert response.status_code == 400
    assert response_json(response)["error"] == "limit must be positive"


@pytest.mark.asyncio
async def test_history_no_ticks_returns_404(monkeypatch):
    monkeypatch.setattr(gateway, "history_store", FakeHistoryStore(ticks=[]))

    response = await gateway.get_history("AAPL", from_ts="1", to_ts="2")

    assert response.status_code == 404
    assert "No history for AAPL" in response_json(response)["error"]


@pytest.mark.asyncio
async def test_history_query_timeout_returns_504(monkeypatch):
    monkeypatch.setattr(
        gateway,
        "history_store",
        FakeHistoryStore(exc=asyncio.TimeoutError()),
    )

    response = await gateway.get_history("AAPL", from_ts="1", to_ts="2")

    assert response.status_code == 504
    assert response_json(response)["error"] == "history query timed out"
