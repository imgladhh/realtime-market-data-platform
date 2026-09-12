import asyncio
import inspect
import json
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response

from src.engine.snapshot_store import SnapshotStore
from src.gateway.session import (
    ClientSession,
    Encoding,
    SequenceDecision,
    SlowConsumerPolicy,
    SubscriptionFilter,
)
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from src.models import EventType, MarketEvent
from src.storage.history_store import DEFAULT_HISTORY_LIMIT, HistoryStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────

snapshot_store = SnapshotStore()
history_store  = HistoryStore()
INSTANCE_ID    = str(uuid.uuid4())[:8]

subscriptions: dict[str, set[ClientSession]] = {}
subscriptions_lock = asyncio.Lock()
pubsub_lock = asyncio.Lock()
all_sessions: dict[str, ClientSession] = {}
active_channels: set[str] = set()
confirmed_channels: set[str] = set()
redis_pubsub = None
INITIALIZATION_BUFFER_MAX_SIZE = 500


@dataclass
class SubscriptionInitialization:
    events: list[MarketEvent] = field(default_factory=list)
    overflowed: bool = False


initializing_subscriptions: dict[
    str, dict[ClientSession, SubscriptionInitialization]
] = {}

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


def _parse_pubsub_event(data) -> MarketEvent | None:
    if isinstance(data, bytes):
        data = data.decode()
    try:
        raw = json.loads(data)
        return MarketEvent(
            symbol=raw["symbol"],
            bid=float(raw["bid"]),
            ask=float(raw["ask"]),
            bid_size=int(raw["bid_size"]),
            ask_size=int(raw["ask_size"]),
            event_ts=int(raw["event_ts"]),
            server_ts=int(raw["server_ts"]),
            seq=int(raw["seq"]),
            type=EventType(raw.get("type", "quote")),
        )
    except Exception as exc:
        logger.error("Failed to parse Redis Pub/Sub event: %s | data=%r", exc, data)
        return None


async def _close_pubsub(pubsub) -> None:
    close = getattr(pubsub, "aclose", None) or pubsub.close
    result = close()
    if inspect.isawaitable(result):
        await result


async def _subscribe_desired_channels(pubsub) -> None:
    confirmed_channels.clear()
    if active_channels:
        channels = sorted(active_channels)
        await pubsub.subscribe(*channels)
        confirmed_channels.update(channels)


async def fanout_loop():
    global _total_events_dispatched, _fanout_start_time, redis_pubsub

    _fanout_start_time = asyncio.get_event_loop().time()
    backoff = 0.1
    logger.info("Redis Pub/Sub fanout loop started")

    while True:
        pubsub = snapshot_store.redis.pubsub(ignore_subscribe_messages=True)
        try:
            async with pubsub_lock:
                redis_pubsub = pubsub
                await _subscribe_desired_channels(pubsub)

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue

                event = _parse_pubsub_event(message.get("data"))
                if event is None:
                    continue

                await _fanout_event(event)
                backoff = 0.1

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Redis Pub/Sub fanout error: %s; reconnecting", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
        finally:
            async with pubsub_lock:
                if redis_pubsub is pubsub:
                    redis_pubsub = None
                    confirmed_channels.clear()
            await _close_pubsub(pubsub)


async def _fanout_event(event: MarketEvent):
    global _total_events_dispatched

    async with subscriptions_lock:
        sessions = subscriptions.get(event.symbol, set()).copy()
        initializations = list(
            initializing_subscriptions.get(event.symbol, {}).values()
        )
        for initialization in initializations:
            if len(initialization.events) >= INITIALIZATION_BUFFER_MAX_SIZE:
                initialization.overflowed = True
            else:
                initialization.events.append(event)

    if not sessions:
        return

    for session in sessions:
        session.aggregator.push(event)
        _total_events_dispatched += 1


def _event_channel(symbol: str) -> str:
    return f"events:{symbol}"


async def _subscribe_pubsub_symbol(symbol: str) -> bool:
    channel = _event_channel(symbol)
    async with pubsub_lock:
        active_channels.add(channel)
        if channel in confirmed_channels:
            return True
        if redis_pubsub is None:
            return False
        try:
            await redis_pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning(
                "Failed to subscribe Redis Pub/Sub channel %s: %s",
                channel,
                exc,
            )
            return False
        confirmed_channels.add(channel)
        return True


async def _ensure_pubsub_subscription(symbol: str) -> bool:
    backoff = 0.05
    for _ in range(5):
        if await _subscribe_pubsub_symbol(symbol):
            return True
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 0.5)
    return False


async def _unsubscribe_pubsub_symbol(symbol: str):
    channel = _event_channel(symbol)
    async with pubsub_lock:
        if channel not in active_channels:
            return
        active_channels.remove(channel)
        confirmed_channels.discard(channel)
        if redis_pubsub is not None:
            try:
                await redis_pubsub.unsubscribe(channel)
            except Exception as exc:
                logger.warning(
                    "Failed to unsubscribe Redis Pub/Sub channel %s: %s",
                    channel,
                    exc,
                )


async def _remove_desired_channel_if_unused(symbol: str) -> None:
    async with subscriptions_lock:
        in_use = bool(subscriptions.get(symbol)) or bool(
            initializing_subscriptions.get(symbol)
        )
    if not in_use:
        await _unsubscribe_pubsub_symbol(symbol)


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

            sequence_decision = session.classify_sequence(
                symbol,
                event.seq,
                detect_gap=is_raw,
            )
            if sequence_decision == SequenceDecision.STALE:
                continue

            if sequence_decision == SequenceDecision.GAP:
                snapshot = await snapshot_store.get(symbol)
                previous_seq = session.last_seq.get(symbol)
                if (
                    snapshot
                    and (previous_seq is None or snapshot.seq > previous_seq)
                    and session.enqueue({"type": "snapshot", **snapshot.to_dict()})
                ):
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
    await snapshot_store.close()
    await history_store.close()
    logger.info("Gateway shutdown")


app = FastAPI(title="Market Data Gateway", lifespan=lifespan)


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/health")
async def get_health():
    try:
        await snapshot_store.ping()
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "reason": "redis unreachable"},
        )
    return {"status": "ok", "instance": INSTANCE_ID}


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

    latency_sessions = [
        s for s in sessions if s.stats.latency.sample_count > 0
    ]
    samples = [
        sample
        for session in latency_sessions
        for sample in session.stats.latency.samples
    ]

    def percentile(values: list[float], p: float) -> float:
        if not values:
            return 0
        ordered = sorted(values)
        index = min(int(len(ordered) * p / 100), len(ordered) - 1)
        return round(ordered[index], 2)

    return {
        "clients": len(sessions),
        "overall_p50_ms": percentile(samples, 50),
        "overall_p99_ms": percentile(samples, 99),
        "average_client_p50_ms": round(
            sum(s.stats.latency.p50 for s in latency_sessions)
            / len(latency_sessions),
            2,
        ) if latency_sessions else 0,
        "average_client_p99_ms": round(
            sum(s.stats.latency.p99 for s in latency_sessions)
            / len(latency_sessions),
            2,
        ) if latency_sessions else 0,
        "latency_samples": len(samples),
        "total_sent": sum(s.stats.sent for s in sessions),
        "total_dropped": sum(s.stats.dropped for s in sessions),
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


@app.get("/history/{symbol}")
async def get_history(
    symbol: str,
    from_ts: str | None = None,
    to_ts: str | None = None,
    limit: str | None = None,
):
    symbol = symbol.upper()
    parsed = _parse_history_params(from_ts, to_ts, limit)
    if isinstance(parsed, JSONResponse):
        return parsed

    start_ts, end_ts, row_limit = parsed
    try:
        ticks = await history_store.fetch_ticks(
            symbol=symbol,
            from_ts=start_ts,
            to_ts=end_ts,
            limit=row_limit,
        )
    except (TimeoutError, asyncio.TimeoutError):
        return JSONResponse(
            status_code=504,
            content={"error": "history query timed out"},
        )

    if not ticks:
        return JSONResponse(
            status_code=404,
            content={"error": f"No history for {symbol}"},
        )

    return {
        "symbol": symbol,
        "from_ts": start_ts,
        "to_ts": end_ts,
        "count": len(ticks),
        "ticks": ticks,
    }


def _parse_history_params(
    from_ts: str | None,
    to_ts: str | None,
    limit: str | None,
) -> tuple[int, int, int] | JSONResponse:
    if from_ts is None or to_ts is None:
        return JSONResponse(
            status_code=400,
            content={"error": "from_ts and to_ts required"},
        )

    try:
        start_ts = int(from_ts)
        end_ts = int(to_ts)
        row_limit = int(limit) if limit is not None else DEFAULT_HISTORY_LIMIT
    except (TypeError, ValueError):
        return JSONResponse(
            status_code=400,
            content={"error": "from_ts, to_ts, and limit must be integers"},
        )

    if start_ts > end_ts:
        return JSONResponse(
            status_code=400,
            content={"error": "from_ts must be <= to_ts"},
        )

    if row_limit <= 0:
        return JSONResponse(
            status_code=400,
            content={"error": "limit must be positive"},
        )

    return start_ts, end_ts, row_limit


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
    if not await _ensure_pubsub_subscription(symbol):
        session.enqueue({"error": f"subscription unavailable for {symbol}"})
        logger.warning(
            "[%s] Subscription to %s failed: Pub/Sub channel not confirmed",
            session.client_id,
            symbol,
        )
        await _remove_desired_channel_if_unused(symbol)
        return False

    subscription_filter = _parse_subscription_filter(filter_payload)
    initialization = SubscriptionInitialization()
    async with subscriptions_lock:
        initializing_subscriptions.setdefault(symbol, {})[session] = initialization

    try:
        snapshot = await snapshot_store.get(symbol)
        async with subscriptions_lock:
            current = initializing_subscriptions.get(symbol, {}).get(session)
            if current is not initialization or initialization.overflowed:
                raise RuntimeError("subscription initialization buffer overflow")

            if snapshot:
                if not session.enqueue({"type": "snapshot", **snapshot.to_dict()}):
                    raise RuntimeError("outbound queue unavailable during subscribe")
                session.last_seq[symbol] = snapshot.seq
                session.last_price[symbol] = snapshot.bid

            for event in sorted(initialization.events, key=lambda item: item.seq):
                if snapshot is None or event.seq > snapshot.seq:
                    session.aggregator.push(event)

            initializing_subscriptions[symbol].pop(session, None)
            if not initializing_subscriptions[symbol]:
                initializing_subscriptions.pop(symbol, None)
            subscriptions.setdefault(symbol, set()).add(session)
            session.subscriptions.add(symbol)
            if subscription_filter is None:
                session.filters.pop(symbol, None)
            else:
                session.filters[symbol] = subscription_filter

        if snapshot:
            logger.info(
                f"[{session.client_id}] Subscribed to {symbol} "
                f"seq_seed={snapshot.seq}"
            )
        else:
            logger.info(f"[{session.client_id}] Subscribed to {symbol} (no snapshot yet)")
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        session.enqueue({"error": f"subscription failed for {symbol}"})
        logger.warning("[%s] Subscription to %s failed: %s", session.client_id, symbol, exc)
        return False
    finally:
        async with subscriptions_lock:
            symbol_initializations = initializing_subscriptions.get(symbol, {})
            symbol_initializations.pop(session, None)
            if not symbol_initializations:
                initializing_subscriptions.pop(symbol, None)
        await _remove_desired_channel_if_unused(symbol)


async def _unsubscribe(session: ClientSession, symbol: str):
    should_unsubscribe = False
    async with subscriptions_lock:
        symbol_sessions = subscriptions.get(symbol, set())
        symbol_sessions.discard(session)
        should_unsubscribe = len(symbol_sessions) == 0
        if should_unsubscribe:
            subscriptions.pop(symbol, None)
    session.subscriptions.discard(symbol)
    session.last_seq.pop(symbol, None)
    session.filters.pop(symbol, None)
    session.last_price.pop(symbol, None)
    if should_unsubscribe:
        await _unsubscribe_pubsub_symbol(symbol)
    logger.info(f"[{session.client_id}] Unsubscribed from {symbol}")


async def _cleanup(session: ClientSession):
    symbols_to_unsubscribe = []
    async with subscriptions_lock:
        for symbol, symbol_initializations in list(initializing_subscriptions.items()):
            symbol_initializations.pop(session, None)
            if not symbol_initializations:
                initializing_subscriptions.pop(symbol, None)
                if not subscriptions.get(symbol):
                    symbols_to_unsubscribe.append(symbol)
        for symbol in session.subscriptions:
            symbol_sessions = subscriptions.get(symbol, set())
            symbol_sessions.discard(session)
            if not symbol_sessions:
                subscriptions.pop(symbol, None)
                symbols_to_unsubscribe.append(symbol)
        all_sessions.pop(session.client_id, None)
    for symbol in symbols_to_unsubscribe:
        await _unsubscribe_pubsub_symbol(symbol)
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
