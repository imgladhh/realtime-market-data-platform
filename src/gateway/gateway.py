import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from src.engine.snapshot_store import SnapshotStore
from src.engine.engine import DistributionEngine
from src.gateway.session import ClientSession, SlowConsumerPolicy
from src.gateway.aggregator import AggregationBuffer, AggregationMode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────

snapshot_store = SnapshotStore()
engine         = DistributionEngine()
aggregator     = AggregationBuffer(mode=AggregationMode.RAW)

subscriptions: dict[str, set[ClientSession]] = {}
subscriptions_lock = asyncio.Lock()
all_sessions: dict[str, ClientSession] = {}

# Global dispatch stats
_total_events_dispatched = 0
_fanout_start_time = 0.0


# ── Fanout loop ───────────────────────────────────────────────────────────────

async def fanout_loop():
    global _total_events_dispatched, _fanout_start_time

    loop = asyncio.get_running_loop()
    engine.consumer.start(loop)
    aggregator.start()
    _fanout_start_time = asyncio.get_event_loop().time()
    logger.info("Fanout loop started")

    async def _ingest():
        """Feed Kafka events into aggregator."""
        async for event in engine.consumer.events():
            await snapshot_store.update(event)
            await aggregator.push(event)

    async def _dispatch():
        """Dispatch aggregated events to subscribers."""
        global _total_events_dispatched
        async for event in aggregator.events():
            async with subscriptions_lock:
                sessions = subscriptions.get(event.symbol, set()).copy()

            if not sessions:
                continue

            message = event.to_dict()
            for session in sessions:
                session.enqueue(message)

            _total_events_dispatched += 1

    # Run both concurrently
    await asyncio.gather(_ingest(), _dispatch())


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fanout_loop())
    logger.info("Gateway started")
    yield
    task.cancel()
    aggregator.stop()
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
    """
    System-wide and per-client metrics.
    Key metrics for interview discussion:
      - dispatch latency p50/p99 per client
      - dropped and coalesced message counts
      - global throughput (events/sec)
    """
    elapsed = asyncio.get_event_loop().time() - _fanout_start_time
    throughput = round(_total_events_dispatched / max(elapsed, 1), 1)

    async with subscriptions_lock:
        clients = {
            sid: s.stats_dict()
            for sid, s in all_sessions.items()
        }

    return {
        "system": {
            "total_dispatched": _total_events_dispatched,
            "uptime_sec":       round(elapsed, 1),
            "throughput_eps":   throughput,   # events per second
            "connected_clients": len(clients),
            "aggregation_mode": aggregator.mode.value,
        },
        "clients": clients,
    }


@app.get("/metrics/summary")
async def get_metrics_summary():
    """Aggregated p50/p99 across all clients."""
    async with subscriptions_lock:
        sessions = list(all_sessions.values())

    if not sessions:
        return {"message": "no connected clients"}

    all_p50 = [s.stats.latency.p50 for s in sessions if s.stats.latency.sample_count > 0]
    all_p99 = [s.stats.latency.p99 for s in sessions if s.stats.latency.sample_count > 0]

    return {
        "clients":      len(sessions),
        "avg_p50_ms":   round(sum(all_p50) / len(all_p50), 2) if all_p50 else 0,
        "avg_p99_ms":   round(sum(all_p99) / len(all_p99), 2) if all_p99 else 0,
        "total_sent":   sum(s.stats.sent for s in sessions),
        "total_dropped": sum(s.stats.dropped for s in sessions),
        "total_coalesced": sum(s.stats.coalesced for s in sessions),
    }


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())[:8]

    # Client can request aggregated mode via query param:
    # ws://localhost:8000/stream?mode=agg_100ms
    mode_param = websocket.query_params.get("mode", "raw")
    coalescing = mode_param != "raw"

    session = ClientSession(
        client_id=client_id,
        websocket=websocket,
        policy=SlowConsumerPolicy.DROP_OLDEST,
        coalescing=coalescing,
    )

    async with subscriptions_lock:
        all_sessions[client_id] = session

    session.start_writer()
    logger.info(f"[{client_id}] Connected (mode={mode_param})")

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
                    await _subscribe(session, symbol)
                elif action == "unsubscribe":
                    await _unsubscribe(session, symbol)
                else:
                    session.enqueue({"error": f"unknown action: {action}"})

    except WebSocketDisconnect:
        logger.info(f"[{client_id}] Disconnected")
    finally:
        await _cleanup(session)


async def _subscribe(session: ClientSession, symbol: str):
    async with subscriptions_lock:
        subscriptions.setdefault(symbol, set()).add(session)
    session.subscriptions.add(symbol)

    snapshot = await snapshot_store.get(symbol)
    if snapshot:
        session.enqueue({"type": "snapshot", **snapshot.to_dict()})
        session.last_seq[symbol] = snapshot.seq

    logger.info(f"[{session.client_id}] Subscribed to {symbol}")


async def _unsubscribe(session: ClientSession, symbol: str):
    async with subscriptions_lock:
        subscriptions.get(symbol, set()).discard(session)
    session.subscriptions.discard(symbol)
    session.last_seq.pop(symbol, None)
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
