# Real-Time Market Data Platform

![CI](https://github.com/imgladhh/realtime-market-data-platform/actions/workflows/ci.yml/badge.svg?branch=master)

A high-throughput market data distribution system that ingests price updates from a simulated feed, routes them through Kafka, and fans out real-time WebSocket streams to multiple subscribers — with bounded latency, controlled backpressure, and consistent state delivery.

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [Architecture Overview](#architecture-overview)
- [Component Deep Dive](#component-deep-dive)
- [Data Flow](#data-flow)
- [Key Design Decisions](#key-design-decisions)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Performance Results](#performance-results)
- [Failure Handling](#failure-handling)
- [Running Locally](#running-locally)
- [Future Extensions](#future-extensions)

---

## What This Project Does

This system solves a specific problem: **how do you reliably deliver high-frequency market data updates to many clients simultaneously, without letting a slow client degrade the experience for fast ones?**

It models a simplified version of what real market data platforms (Bloomberg, Refinitiv) do:

- Upstream produces thousands of price updates per second across many symbols
- Clients subscribe to specific symbols they care about
- Each client needs consistent state — new clients get a full snapshot first, then incremental updates
- The system must not let one slow client block others
- The system must survive client disconnects, reconnects, and burst traffic

---

## Architecture Overview

```
FeedSimulator ──produce, key=symbol──▶ Kafka: market-events
                                         │
                      ┌──────────────────┴──────────────────┐
                      │ group: gateway-engine               │ group: tick-storage
                      ▼                                     ▼
              DistributionEngine                        TickWriter
              update snapshot, publish                  batch ≤500 / ≤100ms
                      │                                     │
                      ▼                                     ▼
              Redis Snapshot + Pub/Sub                 TimescaleDB
                      │                              persistent history
             events:{symbol} broadcast                       ▲
                      │                                      │
              ┌───────┴────────┐                    GET /history/{symbol}
              ▼                ▼                              │
          Gateway 1        Gateway 2 ... ─────────────────────┘
              │                │
        per-client bounded queues, RAW/AGG_100MS, JSON/msgpack
              │                │
              ▼                ▼
            WebSocket clients on any gateway instance
```

---

## Component Deep Dive

### FeedSimulator (`src/feed/simulator.py`)

Simulates an upstream market data feed using a **random walk model**:

```
mid_price(t) = mid_price(t-1) * (1 + N(0, 0.0005))
spread       = mid_price * 0.0002
bid          = mid - spread/2
ask          = mid + spread/2
```

Each event is assigned a **monotonically increasing sequence number within its symbol** before being produced to Kafka. The Kafka `key` is set to the symbol, which determines which partition the message lands in. Sequence numbers do not define ordering across different symbols.

Supported symbols: `AAPL`, `TSLA`, `GOOGL`, `MSFT`, `BTCUSD`

---

### Kafka (`docker-compose.yml`)

Kafka acts as the **durable message bus** between the feed and the distribution engine.

**Topic design:**
- Single topic: `market-events`
- 8 partitions
- Messages keyed by symbol → consistent hashing ensures same symbol always goes to same partition

**Why Kafka instead of an in-memory queue?**

| Concern | In-memory queue | Kafka |
|---------|----------------|-------|
| Producer/consumer decoupling | No | Yes |
| Survives consumer restart | No | Yes (offset replay) |
| Multiple consumer groups | No | Yes |
| Horizontal scaling | Hard | Partition → consumer mapping |
| Audit / replay | No | Yes |

---

### KafkaConsumerBridge (`src/engine/engine.py`)

This is the most technically nuanced component.

**The problem:** `confluent-kafka`'s consumer API is synchronous and blocking. Calling `consumer.poll()` directly in an `async` function would block the entire asyncio event loop, freezing all WebSocket connections.

**The solution:**
```
Thread (blocking Kafka poll loop)
    │
    │  asyncio.run_coroutine_threadsafe(queue.put(envelope), loop)
    │  polling pauses while the bounded handoff is full
    ▼
asyncio event loop (non-blocking queue.get())
    │
    │  Redis snapshot update + Pub/Sub publish
    │  success ACK
    ▼
Kafka thread commits the acknowledged offset
```

The consumer runs in a dedicated daemon thread. Events and Kafka offset metadata are bridged into the asyncio event loop via `asyncio.run_coroutine_threadsafe`. Auto-commit is disabled: the asyncio processor acknowledges an envelope only after both Redis operations succeed, and the consumer thread commits that explicit offset. Waiting for bounded queue capacity prevents unbounded pending handoffs.

---

### SnapshotStore (`src/engine/snapshot_store.py`)

Redis-backed store for the latest state of each symbol.

**Schema:**
```
HSET snapshot:AAPL
  bid       189.11
  ask       189.13
  bid_size  500
  ask_size  300
  seq       102938    ← critical for delta alignment
  ts        1710001234571
```

**Why Redis instead of a Python dict?**

| Concern | Python dict | Redis |
|---------|------------|-------|
| Shared across processes | No | Yes |
| WebSocket gateway scales out | No | Yes |
| Atomic multi-field update | Needs lock | HSET is atomic |
| Persistence across restart | No | AOF / RDB |

---

### ClientSession (`src/gateway/session.py`)

Each connected WebSocket client gets its own `ClientSession` with:

**BoundedQueue:**
```python
asyncio.Queue(maxsize=500)
```
The fanout loop never blocks — it only calls `queue.put_nowait()`. If the queue is full, the `SlowConsumerPolicy` kicks in.

**SlowConsumerPolicy:**

| Policy | Behavior | Use case |
|--------|----------|----------|
| `DROP_OLDEST` | Discard oldest pending message, enqueue latest | Market data (old quotes expire immediately) |
| `DISCONNECT` | Disconnect client after N drops | When data completeness matters |

**LatencyTracker:**
Records end-to-end dispatch latency (`now - event_ts`) only after a WebSocket send completes successfully. Maintains a rolling window of 1000 samples for computing p50/p99 on demand.

**Independent WriterLoop:**
Each session runs its own `asyncio.Task` that drains the queue and writes to the WebSocket. This is why a slow client never affects others — each client's write loop is completely independent.

---

### AggregationBuffer (`src/gateway/aggregator.py`)

Each `ClientSession` owns its own `AggregationBuffer`, selected at connection time via query param.
This means RAW and AGG_100MS clients can coexist on the same gateway simultaneously.

**RAW mode** (default): Every tick is dispatched immediately. Lowest latency, highest bandwidth.

**AGG_100MS mode:** Events are buffered per symbol for 100ms in a per-client buffer.
Only the latest event per symbol is emitted per window. Reduces bandwidth by collapsing burst updates.
Gap detection is disabled in this mode — seq jumps are expected because intermediate events are intentionally skipped.

```
RAW:       AAPL@t1 → AAPL@t2 → AAPL@t3 → ...   (every tick, gap detection on)
AGG_100ms: AAPL@t3 →           AAPL@t7 → ...   (latest per 100ms window, no gap detection)
```

Real-world analogy: professional traders get raw tick data; retail clients get aggregated.

Clients select mode via query param:
```
ws://localhost:8000/stream                  # RAW (default)
ws://localhost:8000/stream?mode=agg_100ms   # per-client 100ms aggregation
```

---

## Data Flow

### New Client Connection

```
1.  Client opens WS connection to /stream
2.  Server creates ClientSession (unique client_id, AggregationBuffer,
    BoundedQueue, WriterLoop)
3.  Client sends: {"action": "subscribe", "symbol": "AAPL"}
4.  Gateway confirms its Redis Pub/Sub subscription to events:AAPL
5.  Gateway marks the client/symbol as initializing
        → Pub/Sub events arriving during initialization are buffered
6.  Gateway reads snapshot:AAPL from Redis
        → snapshot includes seq=N (current per-symbol sequence number)
7.  Gateway enqueues the snapshot and seeds last_seq[AAPL] = N
8.  Gateway discards buffered events with seq <= N, releases newer events
    in sequence order, then registers the client in SubscriptionRegistry[AAPL]
9.  Normal Pub/Sub fanout begins
        → RAW: stale/duplicate seq is discarded; a missing next seq triggers
          a Redis snapshot re-fetch
        → AGG_100MS: forward seq jumps are expected, but stale seq is discarded
```

### Market Event Delivery

```
1.  FeedSimulator generates MarketEvent (seq=X, symbol=AAPL)
2.  Produced to Kafka topic market-events, key=AAPL
3.  Lands in partition determined by hash(AAPL)
4.  KafkaConsumerBridge polls event in background thread
5.  Bridge hands the event to the asyncio loop via run_coroutine_threadsafe
6.  DistributionEngine:
        a. Updates Redis: HSET snapshot:AAPL {..., seq=X}
        b. Publishes the event to Redis channel events:AAPL
        c. Acknowledges the Kafka offset only after both Redis operations succeed
7.  Every gateway instance with local AAPL demand receives the Pub/Sub event
8.  Gateway fanout:
        a. Buffers the event for subscriptions still initializing
        b. Pushes the event into each active session's AggregationBuffer
        c. Applies RAW/AGG_100MS sequencing and subscription filters
        d. Enqueues accepted messages into the per-client BoundedQueue
9.  Each session's WriterLoop independently sends JSON or msgpack to its WebSocket
```

Redis Pub/Sub and bounded client queues provide best-effort live delivery, not
lossless replay. RAW sequence gaps recover the latest coherent state from the
Redis snapshot; replaying every missed tick would require a durable replay path
and client resume offsets.

---

## Key Design Decisions

### Why per-client queues instead of direct send?

**Naive approach:**
```python
for client in subscribers:
    await client.ws.send_text(message)  # blocks if client is slow
```
One slow client blocks the entire fanout loop. All other clients are delayed.

**This system:**
```python
for session in subscribers:
    session.enqueue(message)  # always O(1), never blocks
```
Each client drains its own queue independently. Slow clients are fully isolated.

### Why snapshot + seq alignment?

Without this, a client that connects mid-stream has no idea what the current state is. Receiving `"AAPL bid changed"` is meaningless without knowing what the previous value was.

The snapshot provides the full current state. The seq number on the snapshot tells the client exactly which incremental updates to process — anything with a lower seq is already included in the snapshot.

### Why Kafka partition by symbol?

Partitioning by symbol guarantees that all updates for AAPL arrive in order. Without this, two Kafka partitions could deliver AAPL updates out of sequence, causing the snapshot to be overwritten with stale data.

### Why Redis for snapshots instead of in-memory?

An in-memory dict works for a single process. As soon as you run multiple gateway instances (horizontal scaling), each instance has its own memory and they diverge. Redis provides a shared, consistent view of current state across all gateway instances.

---

## Project Structure

```
.
├── docker-compose.yml              # Kafka + Zookeeper + Redis + Kafka UI
├── requirements.txt
├── README.md
└── src/
    ├── models.py                   # MarketEvent, SnapshotData dataclasses
    ├── feed/
    │   └── simulator.py            # Mock price generator → Kafka producer
    ├── engine/
    │   ├── snapshot_store.py       # Redis HSET read/write
    │   └── engine.py               # KafkaConsumerBridge + DistributionEngine
    ├── gateway/
    │   ├── session.py              # ClientSession, BoundedQueue, LatencyTracker
    │   ├── aggregator.py           # RAW / AGG_100MS aggregation buffer
    │   └── gateway.py              # FastAPI app, WebSocket + REST endpoints
    └── benchmark/
        ├── serialization_bench.py  # JSON vs msgpack comparison
        ├── load_bench.py           # Multi-client latency benchmark
        └── results.md              # Benchmark results with raw output
tests/
    ├── conftest.py                 # Shared fixtures and factories
    ├── test_session.py             # ClientSession unit tests
    ├── test_aggregator.py          # AggregationBuffer unit tests
    ├── test_snapshot_store.py      # SnapshotStore unit tests (Redis mocked)
    └── test_models.py              # MarketEvent model tests
.github/
    └── workflows/
        └── ci.yml                  # GitHub Actions: pytest on every push
```

---

## API Reference

### REST

| Endpoint | Description |
|----------|-------------|
| `GET /snapshot/{symbol}` | Current snapshot for a symbol (reads Redis) |
| `GET /metrics` | Per-client stats: sent, dropped, gaps_detected, p50/p99, queue depth |
| `GET /metrics/summary` | Aggregated stats across all connected clients |

**Snapshot response:**
```json
{
  "symbol": "AAPL",
  "bid": 189.10,
  "ask": 189.12,
  "bid_size": 500,
  "ask_size": 300,
  "seq": 102938,
  "ts": 1710001234567
}
```

### WebSocket `/stream`

**Subscribe:**
```json
{"action": "subscribe", "symbol": "AAPL"}
```

**Unsubscribe:**
```json
{"action": "unsubscribe", "symbol": "AAPL"}
```

**Server → Client: snapshot (on subscribe):**
```json
{
  "type": "snapshot",
  "symbol": "AAPL",
  "bid": 189.10,
  "ask": 189.12,
  "bid_size": 500,
  "ask_size": 300,
  "seq": 102938,
  "ts": 1710001234567
}
```

**Server → Client: incremental update:**
```json
{
  "symbol": "AAPL",
  "bid": 189.11,
  "ask": 189.13,
  "bid_size": 420,
  "ask_size": 310,
  "event_ts": 1710001234600,
  "server_ts": 1710001234604,
  "seq": 102939,
  "type": "quote"
}
```

---

## Performance Results

### Dispatch Latency (load benchmark)

Validated on 2026-09-12 with Docker Desktop/WSL2. The benchmark actively receives
and decodes every frame and computes percentiles from combined client-observed
latency samples. Empty clients, malformed frames, disconnects, or sequence
regressions invalidate the scenario.

50 events/sec per symbol, 2 symbols (AAPL + TSLA), 15s per scenario:

| Clients | p50 (ms) | p99 (ms) | Received | Dropped |
|---------|----------|----------|----------|---------|
| 1       | 8.72     | 10.86    | 1,474    | 0       |
| 5       | 8.96     | 11.12    | 7,424    | 0       |
| 10      | 9.22     | 11.92    | 14,842   | 0       |
| 20      | 9.90     | 13.03    | 29,655   | 0       |

At 20 clients, validated p99 is 13.03ms with zero server-side queue drops,
meeting the project target. See `src/benchmark/results.md` for environment and
measurement details.

### Serialization Benchmark (100,000 iterations)

| Format  | Payload   | Encode         | Decode         | Total  |
|---------|-----------|----------------|----------------|--------|
| JSON    | 204 bytes | 590k ops/sec   | 557k ops/sec   | 349ms  |
| msgpack | 151 bytes | 1,710k ops/sec | 1,627k ops/sec | 120ms  |

**msgpack is 2.91x faster and 26% smaller payload.**

---

## Failure Handling

| Scenario | Behavior |
|----------|----------|
| Client disconnect | Removed from all subscription registries; writer task cancelled cleanly |
| Slow consumer (queue full) | Drop oldest message — in market data, latest value supersedes history |
| Persistent slow consumer | Disconnect after N accumulated drops (configurable threshold) |
| Client reconnect | Re-subscribe flow: fresh snapshot + seq realignment |
| Seq gap detected | RAW client does not receive the next per-symbol seq → fetches a fresh snapshot |
| Kafka consumer lag | Internal bridge queue bounded at 10,000 events |
| Redis restart | AOF persistence restores snapshot data on startup |
| Feed simulator crash | Kafka retains event log; engine resumes from last offset on restart |
| Engine Redis failure | Retries the unacknowledged event with bounded exponential backoff |
| History database unavailable | Returns HTTP 503 without affecting live fanout |
| TickWriter shutdown with a partial batch | Leaves offsets uncommitted for replay on restart |

---

## Testing

The unit suite covers core components with Redis, Kafka, and TimescaleDB
boundaries mocked. Run `pytest -q` for the current count instead of relying on a
hard-coded number in this document.

```bash
pip install pytest pytest-asyncio
pytest -v
```

**Test coverage:**
- `ClientSession`: drop_oldest policy, gap detection, writer loop, latency tracking
- `AggregationBuffer`: RAW passthrough, AGG_100MS buffering, per-symbol latest-wins, flush lifecycle
- `SnapshotStore`: Redis HSET calls, field serialization, TTL, missing key handling
- `MarketEvent`: field preservation, type serialization, to_dict correctness

CI runs automatically on every push via GitHub Actions (`.github/workflows/ci.yml`).

---

## Running Locally

**Prerequisites:** Docker Desktop with WSL2 integration, Python 3.12+

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Set up Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Terminal 1: Start feed simulator
python3 -m src.feed.simulator

# 4. Terminal 2: Start gateway
python3 -m src.gateway.gateway

# 5. Verify
curl http://localhost:8000/snapshot/AAPL
curl http://localhost:8000/metrics/summary

# 6. Run benchmarks
python3 -m src.benchmark.serialization_bench
python3 -m src.benchmark.load_bench

# Or run the complete Kafka/Redis/TimescaleDB validation
bash scripts/run_local_validation.sh
```

**Kafka UI:** http://localhost:8080

---

## Future Extensions

**Kafka offset replay on reconnect**
Clients with a small seq gap replay directly from Kafka offset instead of fetching a full snapshot. Reduces Redis load and provides seamless reconnection for briefly-disconnected clients.

**Security and operational resilience**
Add TLS/authentication, Kafka multi-broker failover, Redis Sentinel/Cluster,
production process supervision, and deployment-level load-balancer automation.
