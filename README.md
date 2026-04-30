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
┌──────────────────────────────────────────────────────────────────┐
│                        FeedSimulator                             │
│   Generates mock price updates via random walk model            │
│   Produces MarketEvent → Kafka (key = symbol)                   │
└────────────────────────┬─────────────────────────────────────────┘
                         │ produce
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                          Kafka                                   │
│   Topic: market-events                                           │
│   Partitioned by symbol hash → per-symbol ordering guaranteed   │
│   Retains event log → supports offset replay on reconnect       │
└────────────────────────┬─────────────────────────────────────────┘
                         │ consume
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                   DistributionEngine                             │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  KafkaConsumerBridge                                    │    │
│  │  - Runs confluent-kafka consumer in dedicated thread    │    │
│  │  - Bridges events into asyncio event loop               │    │
│  │    via asyncio.run_coroutine_threadsafe                  │    │
│  └──────────────────────┬──────────────────────────────────┘    │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐    │
│  │  FanoutDispatcher                                       │    │
│  │  - Updates Redis SnapshotStore                          │    │
│  │  - Looks up SubscriptionRegistry (symbol → sessions)   │    │
│  │  - Pushes into each session's AggregationBuffer        │    │
│  │  - Never blocks: push is always non-blocking            │    │
│  └──────────────────────┬──────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────-┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ClientSession │ │ClientSession │ │ClientSession │
│              │ │              │ │              │
│ AggregationB │ │ AggregationB │ │ AggregationB │
│ RAW or 100ms │ │ RAW or 100ms │ │ RAW or 100ms │
│              │ │              │ │              │
│ BoundedQueue │ │ BoundedQueue │ │ BoundedQueue │
│ (maxsize=500)│ │ (maxsize=500)│ │ (maxsize=500)│
│              │ │              │ │              │
│ WriterLoop   │ │ WriterLoop   │ │ WriterLoop   │
│ (independent)│ │ (independent)│ │ (independent)│
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │ WS             │ WS             │ WS
       ▼                ▼                ▼
    Client A         Client B         Client C

┌──────────────────────────────────────────────────────────────────┐
│                           Redis                                  │
│   SnapshotStore: HSET snapshot:{symbol}                         │
│   Stores latest bid/ask/seq per symbol                          │
│   Read on new client subscribe → snapshot + seq alignment       │
│   AOF persistence: survives Redis restart                       │
└──────────────────────────────────────────────────────────────────┘
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

Each event is assigned a **global monotonically increasing sequence number** before being produced to Kafka. The Kafka `key` is set to the symbol, which determines which partition the message lands in.

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
    │  asyncio.run_coroutine_threadsafe(queue.put(event), loop)
    ▼
asyncio event loop (non-blocking queue.get())
    │
    ▼
FanoutDispatcher
```

The consumer runs in a dedicated daemon thread. Events are bridged into the asyncio event loop via `asyncio.run_coroutine_threadsafe`, which is the only thread-safe way to schedule a coroutine from outside the event loop.

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
Records end-to-end dispatch latency (`now - event_ts`) for every sent message. Maintains a rolling window of 1000 samples for computing p50/p99 on demand.

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
2.  Server creates ClientSession (unique client_id, BoundedQueue, WriterLoop)
3.  Client sends: {"action": "subscribe", "symbol": "AAPL"}
4.  Server reads snapshot:AAPL from Redis
        → snapshot includes seq=N (current sequence number)
5.  Server sends snapshot to client (type="snapshot", seq=N)
6.  Server registers client in SubscriptionRegistry[AAPL]
7.  Fanout loop begins delivering incremental updates (seq > N)
8.  Client checks each incoming seq:
        → gap detected (seq jumps by > 5) → re-subscribe for fresh snapshot
```

### Market Event Delivery

```
1.  FeedSimulator generates MarketEvent (seq=X, symbol=AAPL)
2.  Produced to Kafka topic market-events, key=AAPL
3.  Lands in partition determined by hash(AAPL)
4.  KafkaConsumerBridge polls event in background thread
5.  Bridges into asyncio event loop via run_coroutine_threadsafe
6.  AggregationBuffer: passes through (RAW) or buffers (AGG_100MS)
7.  FanoutDispatcher:
        a. Updates Redis: HSET snapshot:AAPL {..., seq=X}
        b. Looks up SubscriptionRegistry[AAPL] → {session_A, session_B}
        c. session_A.enqueue(event)  ← non-blocking, O(1)
        d. session_B.enqueue(event)  ← non-blocking, O(1)
8.  Each session's WriterLoop independently sends to its WebSocket
```

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

50 events/sec per symbol, 2 symbols (AAPL + TSLA), 15s per scenario:

| Clients | p50 (ms) | p99 (ms) | Total Sent | Dropped |
|---------|----------|----------|------------|---------|
| 1       | 8.33     | 11.25    | 1,274      | 0       |
| 5       | 7.85     | 9.40     | 6,429      | 0       |
| 10      | 7.99     | 9.54     | 12,838     | 0       |
| 20      | 8.38     | 10.22    | 25,687     | 0       |

**Key insight:** p99 latency remains stable (~10ms) from 1 to 20 clients with zero drops. This validates the per-client queue isolation design — fanout does not degrade as client count grows. See `benchmark/results.md` for full output.

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
| Seq gap detected | Client detects jump > 5 in seq → re-subscribes for fresh snapshot |
| Kafka consumer lag | Internal bridge queue bounded at 10,000 events |
| Redis restart | AOF persistence restores snapshot data on startup |
| Feed simulator crash | Kafka retains event log; engine resumes from last offset on restart |

---

## Testing

40 tests covering core components. No external dependencies required (Redis mocked).

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
```

**Kafka UI:** http://localhost:8080

---

## Future Extensions

**Multi-node gateway scaling**
Run multiple gateway instances behind a load balancer. Replace in-memory `SubscriptionRegistry` with Redis Pub/Sub for cross-process fanout. Kafka consumer group ensures each event is processed once regardless of instance count.

**Kafka offset replay on reconnect**
Clients with a small seq gap replay directly from Kafka offset instead of fetching a full snapshot. Reduces Redis load and provides seamless reconnection for briefly-disconnected clients.

**Historical storage**
Add a dedicated `history-writer` Kafka consumer group that asynchronously writes all ticks to PostgreSQL. Completely isolated from the hot path — write latency does not affect real-time delivery.

**True msgpack wire format**
Replace JSON encoding end-to-end. ~26% bandwidth reduction, 2.91x faster serialization at high message rates. Benchmarked and ready to wire in.

**Conditional subscription filters**
Extend subscription to support delivery conditions:
```json
{"action": "subscribe", "symbol": "AAPL", "filter": {"spread_gt": 0.05}}
```
Clients only receive updates when spread widens beyond threshold — closer to real professional data feed behavior.
