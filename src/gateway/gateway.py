import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

from src.engine.snapshot_store import SnapshotStore
from src.engine.engine import DistributionEngine
from src.gateway.session import (
    ClientSession,
    Encoding,
    SlowConsumerPolicy,
    SubscriptionFilter,
)
from src.gateway.aggregator import AggregationBuffer, AggregationMode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────

snapshot_store = SnapshotStore()
engine         = DistributionEngine()

subscriptions: dict[str, set[ClientSession]] = {}
subscriptions_lock = asyncio.Lock()
all_sessions: dict[str, ClientSession] = {}

_total_events_dispatched = 0
_fanout_start_time = 0.0


def _parse_encoding(client_id: str, encoding_param: str | None) -> Encoding:
    try:
        return Encoding(encoding_param or Encoding.JSON.value)
    except ValueError:
        logger.warning(
            f"[{client_id}] Unknown encoding '{encoding_param}', using json"
        )
        return Encoding.JSON


# ── Fanout loop ───────────────────────────────────────────────────────────────

def _parse_subscription_filter(payload) -> SubscriptionFilter | None:
    if not isinstance(payload, dict):
        return None

    min_change_pct = payload.get("min_change_pct")
    max_spread = payload.get("max_spread")
    try:
        return SubscriptionFilter(
            min_change_pct=float(min_change_pct) if min_change_pct is not None else None,
            max_spread=float(max_spread) if max_spread is not None else None,
        )
    except (TypeError, ValueError):
        logger.warning("Invalid subscription filter %r, ignoring", payload)
        return None


async def fanout_loop():
    global _total_events_dispatched, _fanout_start_time

    loop = asyncio.get_running_loop()
    engine.consumer.start(loop)
    _fanout_start_time = asyncio.get_event_loop().time()
    logger.info("Fanout loop started")

    async for event in engine.consumer.events():
        # 1. Update Redis snapshot
        await snapshot_store.update(event)

        # 2. Get subscribers
        async with subscriptions_lock:
            sessions = subscriptions.get(event.symbol, set()).copy()

        if not sessions:
            continue

        # 3. Push into each client's aggregation buffer
        for session in sessions:
            session.aggregator.push(event)
            _total_events_dispatched += 1


async def client_dispatch_loop(session: ClientSession):
    """
    Per-client loop: reads from client's AggregationBuffer,
    runs gap detection, then enqueues into BoundedQueue.

    Gap detection is only meaningful in RAW mode.
    In AGG_100MS mode, seq jumps are expected because aggregation
    intentionally skips intermediate events.
    """
    is_raw = session.aggregator.mode == AggregationMode.RAW
    logger.info(f"[{session.client_id}] Dispatch loop started (mode={session.aggregator.mode.value})")
    try:
        async for event in session.aggregator.events():
            symbol = event.symbol

            # Gap detection: RAW mode only
            if is_raw and session.check_gap(symbol, event.seq):
                snapshot = await snapshot_store.get(symbol)
                if snapshot:
                    session.enqueue({"type": "snapshot", **snapshot.to_dict()})
                    session.last_seq[symbol] = snapshot.seq
                    session.last_price[symbol] = snapshot.bid
                continue

            if not session.should_deliver(event):
                continue

            # Normal incremental delivery
            if session.enqueue(event.to_dict()):
                session.mark_delivered(event)

    except asyncio.CancelledError:
        raise
    finally:
        logger.info(f"[{session.client_id}] Dispatch loop stopped")


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fanout_loop())
    logger.info("Gateway started")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    engine.consumer.stop()
    await snapshot_store.close()
    logger.info("Gateway shutdown")


app = FastAPI(title="Market Data Gateway", lifespan=lifespan)


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/snapshot/{symbol}")
async def get_snapshot(symbol: str):
    symbol = symbol.upper()
    data = await snapshot_store.get(symbol)
    if data is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"No snapshot for {symbol}"}
        )
    return data.to_dict()


@app.get("/metrics")
async def get_metrics():
    elapsed = asyncio.get_event_loop().time() - _fanout_start_time
    throughput = round(_total_events_dispatched / max(elapsed, 1), 1)

    async with subscriptions_lock:
        clients = {
            sid: s.stats_dict()
            for sid, s in all_sessions.items()
        }

    return {
        "system": {
            "total_dispatched":  _total_events_dispatched,
            "uptime_sec":        round(elapsed, 1),
            "throughput_eps":    throughput,
            "connected_clients": len(clients),
        },
        "clients": clients,
    }


@app.get("/metrics/summary")
async def get_metrics_summary():
    async with subscriptions_lock:
        sessions = list(all_sessions.values())

    if not sessions:
        return {"message": "no connected clients"}

    all_p50 = [s.stats.latency.p50 for s in sessions if s.stats.latency.sample_count > 0]
    all_p99 = [s.stats.latency.p99 for s in sessions if s.stats.latency.sample_count > 0]

    return {
        "clients":         len(sessions),
        "avg_p50_ms":      round(sum(all_p50) / len(all_p50), 2) if all_p50 else 0,
        "avg_p99_ms":      round(sum(all_p99) / len(all_p99), 2) if all_p99 else 0,
        "total_sent":      sum(s.stats.sent for s in sessions),
        "total_dropped":   sum(s.stats.dropped for s in sessions),
    }


# ── WebSocket endpoint ────────────────────────────────────────────────────────

# Prometheus metrics endpoint

def _client_label(client_id: str) -> str:
    escaped = client_id.replace("\\", "\\\\").replace('"', '\\"')
    return f'client_id="{escaped}"'


def _append_metric_family(
    lines: list[str],
    name: str,
    help_text: str,
    metric_type: str,
    samples: list[str],
) -> None:
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {metric_type}")
    lines.extend(samples)
    lines.append("")


async def _prometheus_metrics_text() -> str:
    elapsed = asyncio.get_event_loop().time() - _fanout_start_time
    throughput = round(_total_events_dispatched / max(elapsed, 1), 1)

    async with subscriptions_lock:
        sessions = list(all_sessions.values())

    lines: list[str] = []
    _append_metric_family(
        lines,
        "market_data_connected_clients",
        "Current number of connected WebSocket clients",
        "gauge",
        [f"market_data_connected_clients {len(sessions)}"],
    )
    _append_metric_family(
        lines,
        "market_data_events_dispatched_total",
        "Total events dispatched since startup",
        "counter",
        [f"market_data_events_dispatched_total {_total_events_dispatched}"],
    )
    _append_metric_family(
        lines,
        "market_data_throughput_events_per_sec",
        "Current fanout throughput",
        "gauge",
        [f"market_data_throughput_events_per_sec {throughput}"],
    )

    if sessions:
        _append_metric_family(
            lines,
            "market_data_client_sent_total",
            "Total messages sent per client",
            "counter",
            [
                f"market_data_client_sent_total{{{_client_label(s.client_id)}}} {s.stats.sent}"
                for s in sessions
            ],
        )
        _append_metric_family(
            lines,
            "market_data_client_dropped_total",
            "Total messages dropped per client",
            "counter",
            [
                f"market_data_client_dropped_total{{{_client_label(s.client_id)}}} {s.stats.dropped}"
                for s in sessions
            ],
        )

        latency_sessions = [
            s for s in sessions
            if s.stats.latency.sample_count > 0
        ]
        if latency_sessions:
            _append_metric_family(
                lines,
                "market_data_client_latency_p50_ms",
                "Rolling p50 dispatch latency in ms",
                "gauge",
                [
                    f"market_data_client_latency_p50_ms{{{_client_label(s.client_id)}}} {s.stats.latency.p50}"
                    for s in latency_sessions
                ],
            )
            _append_metric_family(
                lines,
                "market_data_client_latency_p99_ms",
                "Rolling p99 dispatch latency in ms",
                "gauge",
                [
                    f"market_data_client_latency_p99_ms{{{_client_label(s.client_id)}}} {s.stats.latency.p99}"
                    for s in latency_sessions
                ],
            )

    return "\n".join(lines).rstrip() + "\n"


@app.get("/metrics/prometheus")
async def get_metrics_prometheus():
    content = await _prometheus_metrics_text()
    return Response(
        content=content,
        headers={"Content-Type": "text/plain; version=0.0.4"},
    )


# WebSocket endpoint

@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())[:8]

    mode_param = websocket.query_params.get("mode", "raw")
    agg_mode = (
        AggregationMode.AGG_100MS
        if mode_param == "agg_100ms"
        else AggregationMode.RAW
    )
    encoding_param = websocket.query_params.get("encoding", "json")
    enc = _parse_encoding(client_id, encoding_param)

    session = ClientSession(
        client_id=client_id,
        websocket=websocket,
        policy=SlowConsumerPolicy.DROP_OLDEST,
        encoding=enc,
    )

    # Assign per-client aggregator and start it
    session.aggregator = AggregationBuffer(mode=agg_mode)
    session.aggregator.start()

    async with subscriptions_lock:
        all_sessions[client_id] = session

    # Start writer loop (drains queue → WebSocket)
    session.start_writer()

    # Start dispatch loop (aggregator → queue)
    dispatch_task = asyncio.create_task(
        client_dispatch_loop(session),
        name=f"dispatch-{client_id}"
    )

    logger.info(f"[{client_id}] Connected (mode={mode_param}, encoding={enc.value})")

    try:
        while True:
            receive_task    = asyncio.create_task(websocket.receive_text())
            disconnect_task = asyncio.create_task(session.wait_until_disconnected())

            done, pending = await asyncio.wait(
                [receive_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()

            if disconnect_task in done:
                break

            if receive_task in done:
                try:
                    raw = receive_task.result()
                except Exception:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    session.enqueue({"error": "invalid json"})
                    continue

                action = msg.get("action")
                symbol = msg.get("symbol", "").upper()

                if not symbol:
                    session.enqueue({"error": "symbol required"})
                    continue

                if action == "subscribe":
                    await _subscribe(session, symbol, msg.get("filter"))
                elif action == "unsubscribe":
                    await _unsubscribe(session, symbol)
                else:
                    session.enqueue({"error": f"unknown action: {action}"})

    except WebSocketDisconnect:
        logger.info(f"[{client_id}] Disconnected")
    finally:
        dispatch_task.cancel()
        try:
            await dispatch_task
        except asyncio.CancelledError:
            pass
        session.aggregator.stop()
        await _cleanup(session)


async def _subscribe(session: ClientSession, symbol: str, filter_payload=None):
    """
    Subscribe flow:
    1. Fetch and send current snapshot with seq=N
    2. Seed last_seq[symbol] = N for gap detection
    3. Register in SubscriptionRegistry so later fanout events are delivered
    """
    snapshot = await snapshot_store.get(symbol)
    if snapshot:
        # Send snapshot directly to queue (not through aggregator)
        session.enqueue({"type": "snapshot", **snapshot.to_dict()})
        session.last_seq[symbol] = snapshot.seq
        session.last_price[symbol] = snapshot.bid
        logger.info(
            f"[{session.client_id}] Subscribed to {symbol} "
            f"seq_seed={snapshot.seq}"
        )
    else:
        logger.info(f"[{session.client_id}] Subscribed to {symbol} (no snapshot yet)")

    async with subscriptions_lock:
        subscriptions.setdefault(symbol, set()).add(session)
        session.subscriptions.add(symbol)
        subscription_filter = _parse_subscription_filter(filter_payload)
        if subscription_filter is None:
            session.filters.pop(symbol, None)
        else:
            session.filters[symbol] = subscription_filter


async def _unsubscribe(session: ClientSession, symbol: str):
    async with subscriptions_lock:
        subscriptions.get(symbol, set()).discard(session)
    session.subscriptions.discard(symbol)
    session.last_seq.pop(symbol, None)
    session.filters.pop(symbol, None)
    session.last_price.pop(symbol, None)
    logger.info(f"[{session.client_id}] Unsubscribed from {symbol}")


async def _cleanup(session: ClientSession):
    async with subscriptions_lock:
        for symbol in session.subscriptions:
            subscriptions.get(symbol, set()).discard(session)
        all_sessions.pop(session.client_id, None)
    await session.close()
    logger.info(f"[{session.client_id}] Cleaned up")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.gateway.gateway:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
