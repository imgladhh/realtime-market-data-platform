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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────

snapshot_store = SnapshotStore()
engine         = DistributionEngine()

# symbol -> set of ClientSession
subscriptions: dict[str, set[ClientSession]] = {}
subscriptions_lock = asyncio.Lock()

# client_id -> ClientSession (for metrics)
all_sessions: dict[str, ClientSession] = {}


# ── Fanout loop ───────────────────────────────────────────────────────────────

async def fanout_loop():
    """
    Core distribution loop.

    For each incoming MarketEvent:
      1. Update Redis snapshot
      2. Look up subscribers for this symbol
      3. Enqueue message into each ClientSession (non-blocking)

    Slow clients are handled inside ClientSession.enqueue(),
    so this loop is never blocked by a single slow client.
    """
    loop = asyncio.get_running_loop()
    engine.consumer.start(loop)
    logger.info("Fanout loop started")

    async for event in engine.consumer.events():
        # 1. Update Redis snapshot
        await snapshot_store.update(event)

        # 2. Get subscribers (non-blocking copy)
        async with subscriptions_lock:
            sessions = subscriptions.get(event.symbol, set()).copy()

        if not sessions:
            continue

        # 3. Enqueue into each client's bounded queue (never blocks)
        message = event.to_dict()
        for session in sessions:
            session.enqueue(message)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fanout_loop())
    logger.info("Gateway started")
    yield
    task.cancel()
    engine.consumer.stop()
    await snapshot_store.close()


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
    """Per-client stats: sent, dropped, uptime."""
    async with subscriptions_lock:
        result = {
            sid: {
                "sent":     s.stats.sent,
                "dropped":  s.stats.dropped,
                "uptime_sec": round(s.stats.uptime_sec, 1),
                "subscriptions": list(s.subscriptions),
                "queue_size": s._queue.qsize(),
            }
            for sid, s in all_sessions.items()
        }
    return {"clients": result, "total": len(result)}


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())[:8]
    session = ClientSession(
        client_id=client_id,
        websocket=websocket,
        policy=SlowConsumerPolicy.DROP_OLDEST,
    )

    async with subscriptions_lock:
        all_sessions[client_id] = session

    session.start_writer()
    logger.info(f"[{client_id}] Client connected")

    try:
        while True:
            # Wait for either a client message or disconnect signal
            receive_task   = asyncio.create_task(websocket.receive_text())
            disconnect_task = asyncio.create_task(session.wait_until_disconnected())

            done, pending = await asyncio.wait(
                [receive_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()

            if disconnect_task in done:
                # Slow consumer triggered disconnect
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
        logger.info(f"[{client_id}] Client disconnected")
    finally:
        await _cleanup(session)


async def _subscribe(session: ClientSession, symbol: str):
    """
    Subscribe flow:
      1. Register in subscriptions registry
      2. Send current snapshot (with seq=N)
      3. Client will receive only seq > N going forward
    """
    async with subscriptions_lock:
        subscriptions.setdefault(symbol, set()).add(session)
    session.subscriptions.add(symbol)

    # Send snapshot immediately so client has current state + seq
    snapshot = await snapshot_store.get(symbol)
    if snapshot:
        session.enqueue({"type": "snapshot", **snapshot.to_dict()})
        # Seed last_seq so gap detection works from here
        session.last_seq[symbol] = snapshot.seq

    logger.info(f"[{session.client_id}] Subscribed to {symbol}")


async def _unsubscribe(session: ClientSession, symbol: str):
    async with subscriptions_lock:
        subscriptions.get(symbol, set()).discard(session)
    session.subscriptions.discard(symbol)
    session.last_seq.pop(symbol, None)
    logger.info(f"[{session.client_id}] Unsubscribed from {symbol}")


async def _cleanup(session: ClientSession):
    """Remove session from all registries on disconnect."""
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
