import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from src.engine.snapshot_store import SnapshotStore
from src.engine.engine import DistributionEngine
from src.models import MarketEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared state
snapshot_store = SnapshotStore()
engine = DistributionEngine()

# Registry: symbol -> set of WebSocket connections
subscriptions: dict[str, set[WebSocket]] = {}
subscriptions_lock = asyncio.Lock()


async def fanout_loop():
    """Read events from engine and push to subscribed WebSocket clients."""
    loop = asyncio.get_running_loop()
    engine.consumer.start(loop)

    async for event in engine.consumer.events():
        # Update Redis snapshot
        await snapshot_store.update(event)

        # Fanout to subscribed clients
        async with subscriptions_lock:
            clients = subscriptions.get(event.symbol, set()).copy()

        if not clients:
            continue

        message = json.dumps(event.to_dict())
        dead = set()
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)

        # Clean up disconnected clients
        if dead:
            async with subscriptions_lock:
                subscriptions[event.symbol] -= dead


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start fanout loop on startup
    task = asyncio.create_task(fanout_loop())
    logger.info("Gateway started, fanout loop running")
    yield
    # Shutdown
    task.cancel()
    engine.consumer.stop()
    await snapshot_store.close()
    logger.info("Gateway shutdown complete")


app = FastAPI(title="Market Data Gateway", lifespan=lifespan)


# ── REST: get current snapshot ────────────────────────────────────────────────

@app.get("/snapshot/{symbol}")
async def get_snapshot(symbol: str):
    symbol = symbol.upper()
    data = await snapshot_store.get(symbol)
    if data is None:
        return JSONResponse(status_code=404, content={"error": f"No snapshot for {symbol}"})
    return data.to_dict()


@app.get("/symbols")
async def get_symbols():
    return {"symbols": list(engine.consumer._config.get("symbols", [
        "AAPL", "TSLA", "GOOGL", "MSFT", "BTCUSD"
    ]))}


# ── WebSocket: subscribe to real-time updates ─────────────────────────────────

@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    client_symbols: set[str] = set()
    logger.info("Client connected")

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            action = msg.get("action")
            symbol = msg.get("symbol", "").upper()

            if not symbol:
                await websocket.send_text(json.dumps({"error": "symbol required"}))
                continue

            if action == "subscribe":
                async with subscriptions_lock:
                    subscriptions.setdefault(symbol, set()).add(websocket)
                client_symbols.add(symbol)

                # Send current snapshot immediately on subscribe
                snapshot = await snapshot_store.get(symbol)
                if snapshot:
                    await websocket.send_text(json.dumps({
                        "type": "snapshot",
                        **snapshot.to_dict()
                    }))
                logger.info(f"Client subscribed to {symbol}")

            elif action == "unsubscribe":
                async with subscriptions_lock:
                    subscriptions.get(symbol, set()).discard(websocket)
                client_symbols.discard(symbol)
                logger.info(f"Client unsubscribed from {symbol}")

            else:
                await websocket.send_text(json.dumps({"error": f"unknown action: {action}"}))

    except WebSocketDisconnect:
        logger.info("Client disconnected")
    finally:
        # Clean up all subscriptions for this client
        async with subscriptions_lock:
            for symbol in client_symbols:
                subscriptions.get(symbol, set()).discard(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.gateway.gateway:app", host="0.0.0.0", port=8000, reload=False)