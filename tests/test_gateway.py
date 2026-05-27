import asyncio
import time

import pytest

from src.gateway import gateway
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from src.gateway.session import ClientSession, Encoding
from src.models import SnapshotData
from tests.conftest import make_websocket


def make_session(client_id: str = "gateway-test") -> ClientSession:
    session = ClientSession(client_id=client_id, websocket=make_websocket())
    session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
    return session


def response_text(response) -> str:
    return response.body.decode()


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


def test_encoding_param_msgpack():
    assert gateway._parse_encoding("gateway-test", "msgpack") == Encoding.MSGPACK


def test_encoding_param_default_is_json():
    assert gateway._parse_encoding("gateway-test", None) == Encoding.JSON


def test_encoding_param_invalid_falls_back_to_json(caplog):
    with caplog.at_level("WARNING"):
        assert gateway._parse_encoding("gateway-test", "protobuf") == Encoding.JSON

    assert "Unknown encoding 'protobuf'" in caplog.text


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
