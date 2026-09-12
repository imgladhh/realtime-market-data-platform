# Real-Time Market Data Platform — Technical Specification

---

## 1. Problem Statement

Design and implement a real-time market data distribution system that ingests high-frequency price updates from upstream sources and fans them out to multiple WebSocket subscribers — with bounded latency, controlled backpressure, consistent state delivery, and isolation between fast and slow consumers.

This system models the core infrastructure behind platforms like Bloomberg Terminal and Refinitiv Eikon: receiving thousands of price ticks per second, routing them to the right subscribers, and ensuring every client sees a coherent, up-to-date market view regardless of when they connect or how fast they consume.

---

## 2. Requirements

### 2.1 Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-1 | Support multiple symbols (AAPL, TSLA, GOOGL, MSFT, BTCUSD) |
| FR-2 | Clients subscribe/unsubscribe to specific symbols via WebSocket |
| FR-3 | New clients receive a full snapshot of current state on subscribe |
| FR-4 | After snapshot, clients receive incremental updates only |
| FR-5 | Snapshot and incremental updates are aligned via sequence numbers |
| FR-6 | Clients detect gaps in sequence and can request re-snapshot |
| FR-7 | Support two delivery modes: RAW (every tick) and AGG_100MS (100ms window) |
| FR-8 | REST endpoint to fetch current snapshot for any symbol |
| FR-9 | REST endpoint to query per-client and system-wide metrics |
| FR-10 | Clients select outbound encoding at connect time via `?encoding=` query param (`json` default, `msgpack` binary frames); unknown values fall back to `json` |
| FR-11 | `GET /metrics/prometheus` exposes system and per-client metrics in Prometheus text format 0.0.4 |
| FR-12 | Every `MarketEvent` consumed from Kafka must be written to persistent tick storage |
| FR-13 | `GET /history/{symbol}?from_ts=<ms>&to_ts=<ms>&limit=<n>` returns ticks ordered ascending by `event_ts`; filters by `event_ts` range |
| FR-14 | Duplicate ticks (same `symbol` + `seq` + `event_time`) are silently ignored on insert (`ON CONFLICT DO NOTHING`) |
| FR-15 | Tick writer runs as independent Kafka consumer group `tick-storage`, decoupled from `gateway-engine` group |
| FR-16 | The history endpoint is read-only; it does not affect the live fanout path |
| FR-17 | Multiple gateway instances can run concurrently; clients connect to any instance |
| FR-18 | A client reconnecting to a different instance receives a correct snapshot and resumes incremental delivery |
| FR-19 | An instance failure disconnects only its own clients; other instances and their clients are unaffected |
| FR-20 | Events published to Redis Pub/Sub are distributed to all gateway instances subscribed to that symbol |
| FR-21 | `GET /health` returns 200 while the instance is ready; returns 503 when Redis is unreachable |
| FR-22 | Prometheus metrics are per-instance; `instance` label is applied at the Prometheus scrape level |

### 2.2 Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-1 | p99 dispatch latency < 15ms under 20 concurrent clients |
| NFR-2 | Zero message drops under normal load (50 events/sec per symbol, 5 symbols, 20 clients) |
| NFR-3 | Slow clients must not degrade performance for fast clients |
| NFR-4 | System must survive client disconnect/reconnect gracefully |
| NFR-5 | Feed simulator crash must not take down the distribution engine |
| NFR-6 | Snapshot data must survive Redis restart (AOF persistence) |
| NFR-7 | msgpack-encoded payloads must be ≥ 20% smaller than JSON equivalents for a standard MarketEvent |
| NFR-8 | Tick write throughput must sustain ≥ 50 000 events/sec without backpressure on the fanout loop |
| NFR-9 | History query p99 latency < 200ms for a 1-hour window on a single symbol |
| NFR-10 | Tick writer failures must not crash the gateway process; the writer retries with bounded back-off (max 5s) |
| NFR-11 | Adding a second gateway instance must not increase single-client p99 dispatch latency beyond NFR-1 (15ms) |
| NFR-12 | Total client capacity scales linearly with instance count under uniform subscription load |
| NFR-13 | Instance restart must complete within 5 seconds without data loss to other instances |

---

## 3. Architecture

### 3.1 System Overview

```
┌──────────────┐     ┌───────┐     ┌───────────────────┐
│ FeedSimulator│────▶│ Kafka │────▶│ DistributionEngine│
│ (producer)   │     │       │     │ (separate process)│
└──────────────┘     └───────┘     └────────┬──────────┘
                           │                │ update + publish
                     group:│                ▼
                     tick- │        ┌──────────────────────┐
                     stor. │        │         Redis        │
                           │        │  snapshot:{symbol}   │
                           │        │  events:{symbol}     │ ◀── Pub/Sub
                           │        └───────────┬──────────┘
                           │                    │ subscribe (per symbol, on demand)
                           │          ┌─────────┴─────────┐
                           │          │                   │
                           │  ┌───────▼──────┐   ┌───────▼──────┐
                           │  │  Gateway-1   │   │  Gateway-2   │  ...
                           │  │  fanout_loop │   │  fanout_loop │
                           │  │  sessions    │   │  sessions    │
                           │  └──────────────┘   └──────────────┘
                           │          │                   │
                           │          └────────┬──────────┘
                           │                   │
                           │       ┌───────────▼──────────┐
                           │       │   Load Balancer       │
                           │       │ (WebSocket sticky,    │
                           │       │  /health liveness)    │
                           │       └───────────────────────┘
                           │
                    ┌──────▼─────────┐
                    │   TickWriter   │
                    └───────┬────────┘
                            │ bulk INSERT
                            ▼
                    ┌────────────────┐
                    │  TimescaleDB   │◀── GET /history/{symbol}
                    │ (market_ticks) │
                    └────────────────┘
```

### 3.2 Component Inventory

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| FeedSimulator | Generate mock price events, produce to Kafka | Python, confluent-kafka Producer |
| Kafka | Durable message bus, partition by symbol | Confluent Kafka 7.6.0, 8 partitions |
| Zookeeper | Kafka cluster coordination | Confluent Zookeeper 7.6.0 |
| DistributionEngine | Consume from Kafka, update snapshots, publish to Redis Pub/Sub | Python asyncio, threading bridge (separate process from gateway) |
| SnapshotStore | Store latest state per symbol | Redis 7.2, HSET, AOF persistence |
| WebSocket Gateway | Client connections, subscribe/unsubscribe, REST | FastAPI, uvicorn |
| ClientSession | Per-client queue, writer loop, latency tracking | asyncio.Queue, asyncio.Task |
| AggregationBuffer | Per-client RAW or 100ms windowed delivery | asyncio flush loop |
| Load Balancer | Routes WebSocket and REST traffic; WebSocket sticky sessions; `/health` liveness probe | Deployment-level nginx or equivalent; not part of local compose |
| TickWriter | Independent Kafka consumer (`tick-storage` group), bulk-writes ticks to TimescaleDB | Python asyncio, confluent-kafka |
| HistoryStore | Async read layer for tick history; schema init and bulk insert | asyncpg connection pool |
| TimescaleDB | Time-series storage for market ticks, partitioned by `event_time` | TimescaleDB 2.x on PostgreSQL |
| Kafka UI | Debugging and observability | provectuslabs/kafka-ui |

### 3.3 Data Flow

**Event Ingestion:**
```
FeedSimulator
  → MarketEvent(symbol, bid, ask, sizes, event_ts, server_ts, seq, type)
  → Kafka produce(topic="market-events", key=symbol, value=JSON)
  → Lands in partition = hash(symbol) % 8
```

**Event Distribution (DistributionEngine — separate process):**
```
KafkaConsumerBridge (background thread)
  → consumer.poll() [blocking, in dedicated thread]
  → asyncio.run_coroutine_threadsafe(queue.put(envelope), loop)
  → bounded handoff blocks polling while the queue is full
  → asyncio event loop receives event + Kafka offset metadata
  → SnapshotStore.update(event) → Redis HSET snapshot:{symbol}
  → SnapshotStore.publish_event(event) → Redis PUBLISH events:{symbol} <json>
  → acknowledge success to the consumer thread
  → consumer thread coalesces contiguous acknowledgements per partition
  → one synchronous commit advances each partition to its highest safe offset
```

**Event Fanout (Gateway — per instance):**
```
fanout_loop (asyncio.Task, one per gateway instance)
  → Redis Pub/Sub subscribe to events:{symbol} (on-demand, per first local subscriber)
  → pubsub.listen() [async generator]
  → _fanout_event(event):
      → For each local ClientSession subscribed to event.symbol:
          → session.aggregator.push(event) [non-blocking]

Gateway subscribes to a channel when the first local client subscribes to a symbol.
Gateway unsubscribes when the last local client unsubscribes.
```

**Client Delivery:**
```
client_dispatch_loop (per-client asyncio.Task)
  → AggregationBuffer.events() [async generator]
  → classify sequence as NEXT / GAP / STALE
  → RAW: exact next sequence required; AGG_100MS: forward jumps allowed
  → session.enqueue(event.to_dict()) [BoundedQueue, non-blocking]

WriterLoop (per-client asyncio.Task)
  → queue.get() [async, non-blocking]
  → ws.send_text(json.dumps(message))
  → LatencyTracker.record(event_ts)
```

---

## 4. Data Model

### 4.1 MarketEvent

The core unit of data flowing through the system.

```python
@dataclass
class MarketEvent:
    symbol:     str          # "AAPL", "TSLA", etc.
    bid:        float        # best bid price
    ask:        float        # best ask price
    bid_size:   int          # bid quantity
    ask_size:   int          # ask quantity
    event_ts:   int          # upstream timestamp (ms since epoch)
    server_ts:  int          # ingestion timestamp (ms since epoch)
    seq:        int          # monotonic sequence number within symbol
    type:       EventType    # QUOTE | TRADE | SNAPSHOT
    last_price: float = 0.0  # last trade price (if type=TRADE)
    last_size:  int   = 0    # last trade size
```

**Key invariants:**
- `seq` is monotonically increasing within each symbol, assigned at the simulator
- `seq` does not define ordering between different symbols
- `event_ts` and `server_ts` difference = ingestion latency
- `type` is always "quote" for incremental updates

### 4.2 SnapshotData

Stored in Redis per symbol. Subset of MarketEvent.

```python
@dataclass
class SnapshotData:
    symbol:   str
    bid:      float
    ask:      float
    bid_size: int
    ask_size: int
    seq:      int    # critical: used for delta alignment
    ts:       int    # timestamp of last update
```

**Redis schema:**
```
Key:     snapshot:{symbol}
Type:    HASH
Fields:  bid, ask, bid_size, ask_size, seq, ts
TTL:     86400 seconds (24 hours)
Update:  HSET (atomic across all fields, pipeline+transaction)

Channel: events:{symbol}
Type:    Pub/Sub (ephemeral, no persistence)
Message: JSON-serialised MarketEvent
Publisher: DistributionEngine (after each snapshot update)
Subscribers: Gateway instances (on-demand, per active symbol)
```

`MARKET_DATA_NAMESPACE` optionally prefixes snapshot keys and Pub/Sub channels.
The local validation script uses a unique namespace per run so benchmark state
cannot collide with existing Redis data.

### 4.3 market_ticks (TimescaleDB hypertable)

Persistent tick storage, one row per market event.

```sql
CREATE TABLE market_ticks (
    event_time  TIMESTAMPTZ NOT NULL,   -- derived from event_ts (ms); hypertable partition column
    symbol      TEXT        NOT NULL,
    seq         BIGINT      NOT NULL,
    bid         DOUBLE PRECISION NOT NULL,
    ask         DOUBLE PRECISION NOT NULL,
    bid_size    INTEGER     NOT NULL,
    ask_size    INTEGER     NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()  -- wall-clock write time; ops/lag only
);
```

Indexes:
- `UNIQUE (symbol, seq, event_time)` — deduplication; must include partition column for TimescaleDB
- `(symbol, event_time)` — range query support

**Key invariants:**
- `event_time` is always derived from `event_ts` (never from `received_at`)
- `ON CONFLICT DO NOTHING` makes all inserts idempotent
- Schema creation is owned by TickWriter via `HistoryStore.ensure_schema()`; gateway calls no DDL

### 4.4 Gateway Per-Instance State (Phase 3 additions)

```
INSTANCE_ID:    str (uuid4[:8]) — stable for the life of the process; returned by /health
active_channels: set[str]       — desired Redis Pub/Sub channels for local demand
confirmed_channels: set[str]    — channels confirmed on the current Pub/Sub connection
redis_pubsub:   PubSub | None   — active Pub/Sub connection; None while reconnecting
pubsub_lock:    asyncio.Lock    — guards desired/confirmed channel state and redis_pubsub
```

**Invariant:** `active_channels` represents desired local demand; a channel enters
`confirmed_channels` only after Redis confirms `SUBSCRIBE`. Reconnect clears and
rebuilds the confirmed set from desired channels. Client initialization uses a
bounded per-session/symbol event buffer until its snapshot boundary is installed.

### 4.5 ClientSession

In-memory, one per connected WebSocket client.

```
ClientSession
  ├── client_id:      str (uuid4[:8])
  ├── websocket:      WebSocket
  ├── subscriptions:  set[str]
  ├── last_seq:       dict[str, int]     # per-symbol, seeded from snapshot
  ├── encoding:       Encoding           # JSON (text) or MSGPACK (binary); set at connect
  ├── stats:          ClientStats
  │     ├── sent:          int
  │     ├── dropped:       int
  │     ├── gaps_detected: int
  │     └── latency:       LatencyTracker (rolling 1000 samples)
  ├── aggregator:     AggregationBuffer  # RAW or AGG_100MS
  ├── _queue:         asyncio.Queue(500) # bounded outbound queue
  ├── _writer_task:   asyncio.Task       # independent drain loop
  └── _disconnected:  asyncio.Event      # signals forced disconnect
```

---

## 5. Key Design Decisions

### 5.1 Kafka Partition Strategy

```
key = symbol → partition = hash(symbol) % num_partitions
```

**Why:** All events for AAPL land on the same partition, guaranteeing strict ordering within a symbol. Different symbols are processed in parallel across partitions.

**Tradeoff:** Hot symbols (e.g., AAPL gets 10x the updates of BTCUSD) create partition skew. For this system with 5 symbols and 8 partitions, skew is acceptable. In production with 10,000 symbols, you'd monitor partition lag and rebalance.

### 5.2 Snapshot + Incremental Update Protocol

**Problem:** A client connecting mid-stream has no context. Receiving "AAPL bid=189.11" is meaningless without knowing the ask, sizes, or where in the sequence this falls.

**Solution:**
```
Client connects → subscribes to AAPL
  → Server confirms Redis subscription to events:AAPL
  → Server marks the client/symbol as initializing and buffers observed events
  → Server reads snapshot:AAPL from Redis (seq=N)
  → Server sends snapshot to client (type="snapshot")
  → Server seeds client.last_seq["AAPL"] = N
  → Server discards buffered seq <= N and releases buffered seq > N in order
  → Server registers client in SubscriptionRegistry["AAPL"]
  → Dispatch loop delivers events with seq > N
  → Client checks each per-symbol seq: expected N+1; a larger value triggers re-snapshot
```

**Why seq=N matters:** Without it, the client doesn't know which incremental updates overlap with the snapshot. seq creates a deterministic boundary.

### 5.3 Per-Client Bounded Queue

**Problem:** `await ws.send_text()` blocks if the client's TCP buffer is full. One slow client blocks the entire fanout loop.

**Solution:** Each client has an `asyncio.Queue(maxsize=500)`. The fanout loop only calls `put_nowait()` — O(1), never blocks. Each client has its own writer task doing `await ws.send_text()` independently.

**Drop policy:** When queue is full, discard the oldest message and enqueue the newest. For market data, old quotes expire immediately — the latest price is always more valuable than completeness.

### 5.4 asyncio + Kafka Thread Bridge

**Problem:** `confluent-kafka` is synchronous. `consumer.poll()` blocks the calling thread.

**Solution:**
```
Thread: while True: msg = consumer.poll(0.1)
          → asyncio.run_coroutine_threadsafe(queue.put(envelope), loop)
          → stop polling while the bounded handoff is full

asyncio: async for envelope in queue:
           update Redis snapshot
           publish Redis event
           acknowledge the Kafka offset to the consumer thread
```

Kafka auto-commit is disabled. The consumer is called only from its owner thread,
and an offset is committed only after both Redis operations succeed.

**Why not aiokafka:** confluent-kafka wraps librdkafka (C library) — faster and more production-proven. The thread bridge adds ~1ms overhead, negligible vs. total dispatch latency.

### 5.5 Per-Client Aggregation

**Problem:** Professional clients want every tick. Retail clients only need updates every 100ms.

**Solution:** Each ClientSession owns its own AggregationBuffer, selected at connection time:
```
ws://localhost:8000/stream               → RAW
ws://localhost:8000/stream?mode=agg_100ms → 100ms window
```

RAW and AGG_100MS clients coexist on the same gateway. RAW mode enables gap detection; AGG_100MS disables it (seq jumps are expected).

### 5.7 Encoding Enum Over Raw String

Outbound WebSocket encoding is represented as `Encoding(str, Enum)` with values `JSON = "json"` and `MSGPACK = "msgpack"`, following the same pattern as `AggregationMode` and `SlowConsumerPolicy`.

**Why:** A raw string field allows half-supported states (`"Msgpack"`, `"protobuf"`) to silently fall through to JSON. An enum creates a hard boundary: the gateway parses the `?encoding=` query param with a try/except, falls back to `Encoding.JSON` on `ValueError`, and logs a WARNING. Future encodings require an explicit enum entry.

**Wire format dispatch** in `_writer_loop`:
```python
if self.encoding == Encoding.MSGPACK:
    await websocket.send_bytes(msgpack.packb(message, use_bin_type=True))
else:
    await websocket.send_text(json.dumps(message))
```

`use_bin_type=True` ensures Python `str` fields encode as UTF-8 strings (msgpack type 5), not bytes (type 2). Inbound messages (subscribe/unsubscribe) remain JSON regardless of encoding.

### 5.9 Phase 3: Redis Pub/Sub as the Inter-Instance Broadcast Layer

**Decision:** DistributionEngine publishes to `events:{symbol}` after each snapshot update. Gateway instances subscribe to Redis Pub/Sub instead of consuming from Kafka directly. The engine runs as a separate process from the gateway.

**Why:** Decouples Kafka consumer count from gateway instance count. One engine reads the full Kafka stream; N gateway instances each receive only the symbols their clients are watching — no per-instance Kafka consumer groups, no full-stream duplication. The gateway becomes stateless with respect to Kafka.

**Tradeoff:** Redis Pub/Sub is fire-and-forget. A gateway instance that is slow, restarting, or briefly disconnected from Redis misses messages in-flight. In RAW mode, a missing next per-symbol sequence triggers a snapshot re-fetch from Redis, which is persistent. Net effect: a reconnecting gateway client sees one snapshot re-seed rather than a hard failure.

---

**Decision:** On-demand Pub/Sub channel management — subscribe when the first local client subscribes to a symbol, unsubscribe when the last local client leaves.

**Why:** Avoids subscribing to symbols that have no local subscribers (would waste Redis bandwidth and CPU on `_fanout_event` no-ops). Particularly important when many gateway instances run with different client populations.

**Tradeoff:** Subscribe/unsubscribe calls go to Redis on the path of client subscribe/unsubscribe. Both are protected by `pubsub_lock` and are async; they do not block the WebSocket receive loop.

---

**Decision:** WebSocket sticky sessions at the load balancer (source-IP hash or cookie-based affinity).

**Why:** Per-client state (`subscriptions`, `last_seq`, `aggregator`, `_queue`) is in-process. Moving a live session across instances requires session migration, which is out of scope. Sticky sessions ensure the session lives on one instance for its full duration.

**Tradeoff:** A single instance failure drops all its sticky connections. Clients reconnect; the load balancer distributes them across surviving instances. No data loss — snapshot recovery via Redis on reconnect. Consistent with NFR-13 (instance restart < 5s).

---

**Decision:** DistributionEngine remains a single-group Kafka consumer in this phase. It is not run inside the gateway process.

**Why:** The engine's work (snapshot HSET + Pub/Sub publish) is I/O-bound and sustains 50k events/sec on one process. Active-active engine redundancy requires leader election and is deferred. By removing the engine from the gateway process, the gateway binary is stateless with respect to Kafka and can be restarted freely.

**Tradeoff:** The engine remains a SPOF for event delivery. An engine crash means no new events reach any gateway instance until it restarts; connected clients coast on stale data (gap detection fires on resume).

### 5.8 Phase 2b: Independent Tick Storage Consumer

**Decision:** TickWriter uses Kafka consumer group `tick-storage`, separate from the live fanout group `gateway-engine`.

**Why:** Tick storage lag must not throttle live dispatch. The two groups have independent offsets, different durability guarantees (storage must persist; fanout is ephemeral), and different failure modes. An overloaded database cannot delay live event delivery.

**Tradeoff:** Two consumer connections to Kafka; partition rebalances are independent events.

---

**Decision:** `event_time` (= `event_ts`) is the TimescaleDB partition column and the axis for all range queries. `received_at` is stored but never filtered on.

**Why:** `event_ts` is producer-stamped and deterministic. `received_at` varies with consumer lag and is opaque to callers. Per FR-13, the API contract is event-time filtering.

**Tradeoff:** A tick with a stale or skewed `event_ts` (e.g., replayed from Kafka) may land in an unexpected time chunk. The per-symbol `seq` plus the deduplication index prevents double-counting for an identical event identity.

---

**Decision:** Bulk insert via `INSERT ... SELECT * FROM UNNEST(...) ON CONFLICT DO NOTHING`, batching up to 500 rows or 100ms, whichever comes first.

**Why:** Row-by-row inserts cannot sustain 50k events/sec against PostgreSQL. UNNEST-based bulk insert keeps batching logic in Python (no COPY protocol complexity) while achieving high throughput.

**Tradeoff:** Up to 100ms write lag before ticks are visible in `/history`.

---

**Decision:** On `TimeoutError` or another write exception, the writer retries with exponential back-off (max 5s) and does not commit until the write succeeds.

**Why:** A timeout leaves the write result unknown. Retrying is safe because the database insert uses `ON CONFLICT DO NOTHING`; committing an unresolved batch would permanently lose Kafka records.

### 5.6 Why Coalescing Is NOT in ClientSession

Early implementation had a `_pending` dict in ClientSession that tracked the latest message per symbol. When a new message arrived for a symbol already in the queue, it updated `_pending` but could not replace the already-enqueued old message (asyncio.Queue does not support in-place replacement). The writer dequeued the stale message, cleared `_pending`, and the latest message was silently lost.

**Fix:** Remove ClientSession coalescing entirely. Let AggregationBuffer handle latest-per-symbol logic before events enter the queue.

---

## 6. Failure Handling

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| Client disconnect | WebSocketDisconnect exception | Remove from all subscription registries, cancel writer task, cancel dispatch task, close aggregator |
| Slow consumer (queue full) | asyncio.QueueFull in enqueue | Drop oldest message, increment dropped counter |
| Persistent slow consumer | stats.dropped > threshold | Proactive disconnect via _disconnected event |
| Sequence gap | RAW sequence is greater than `last_seq + 1` | Re-fetch snapshot from Redis, re-seed last_seq, skip stale event |
| Duplicate/stale sequence | Sequence is less than or equal to `last_seq` | Discard without moving the sequence boundary backward |
| Kafka consumer lag | Internal bridge queue bounded at 10,000 | Consumer polling pauses while handoff is full; asyncio loop remains unaffected |
| Redis restart | AOF persistence | Snapshot restored on startup; Kafka replay fills gaps within seconds |
| Feed simulator crash | Kafka retains event log | Engine resumes from last committed offset on restart |
| Gateway restart | All WebSocket connections drop | Clients reconnect, re-subscribe, get fresh snapshot |
| TimescaleDB unavailable at TickWriter start | Connection error on `ensure_schema` | Exponential back-off retry; Kafka offset not committed until write succeeds |
| Tick batch write timeout | `asyncio.TimeoutError` from asyncpg | Retry with bounded back-off; offset remains uncommitted until success |
| Tick batch write error (non-timeout) | Exception from `insert_ticks` | Retry with back-off up to 5s per attempt; offset not committed until success |
| Duplicate tick on retry | `ON CONFLICT DO NOTHING` | Silently skipped; insert is idempotent |
| HistoryStore DB unavailable at query time | asyncpg connection/pool error | Returns HTTP 503 to caller; no fanout impact |
| TickWriter consumer lag | Kafka consumer group lag metric | Operational alert only; no impact on `gateway-engine` group |
| Gateway instance crash | Load balancer `/health` probe fails | Sticky clients reconnect; routed to surviving instances; re-subscribe yields fresh snapshot |
| Redis Pub/Sub disconnect in gateway | Exception in `pubsub.listen()` | `fanout_loop` reconnects with exponential back-off (max 5s), clears confirmations, and re-subscribes every desired channel |
| DistributionEngine Redis operation fails | Exception from snapshot update or publish | Retry the same unacknowledged event with exponential back-off (max 5s); shutdown leaves it replayable |
| Pub/Sub subscribe fails | Exception in `_subscribe_pubsub_symbol` | Bounded immediate retry; the channel is never marked confirmed on failure and the initiating client is rolled back if retries fail |
| Thundering herd on instance restart | All sticky clients reconnect simultaneously | Snapshot fetches serialise through Redis; existing per-client bounded queue behaviour unchanged |

---

## 7. API Specification

### 7.1 REST Endpoints

**GET /health**

Liveness probe used by the load balancer.

```
Request:  GET /health
Response: 200
{"status": "ok", "instance": "a3f2c1d9"}

Response: 503 (Redis unreachable)
{"status": "degraded", "reason": "redis unreachable"}
```

**GET /snapshot/{symbol}**

Returns current snapshot from Redis.

```
Request:  GET /snapshot/AAPL
Response: 200
{
  "symbol": "AAPL",
  "bid": 189.10,
  "ask": 189.12,
  "bid_size": 500,
  "ask_size": 300,
  "seq": 102938,
  "ts": 1710001234567
}

Response: 404 (symbol not found)
{"error": "No snapshot for XYZ"}
```

**GET /metrics**

Per-client stats.

```
{
  "system": {
    "total_dispatched": 45230,
    "uptime_sec": 120.5,
    "throughput_eps": 376.1,
    "connected_clients": 3
  },
  "clients": {
    "f023ce3e": {
      "sent": 1200,
      "dropped": 0,
      "gaps_detected": 0,
      "uptime_sec": 45.2,
      "subscriptions": ["AAPL", "TSLA"],
      "queue_size": 0,
      "aggregation_mode": "raw",
      "latency_ms": {
        "p50": 8.12,
        "p99": 11.46,
        "samples": 1000
      }
    }
  }
}
```

**GET /metrics/prometheus**

Returns current metrics in Prometheus text format 0.0.4. Only currently-connected clients appear in per-client label series; disconnected clients are automatically absent.

```
Content-Type: text/plain; version=0.0.4

# HELP market_data_connected_clients Current number of connected WebSocket clients
# TYPE market_data_connected_clients gauge
market_data_connected_clients 3

# HELP market_data_events_dispatched_total Total events dispatched since startup
# TYPE market_data_events_dispatched_total counter
market_data_events_dispatched_total 45230

# HELP market_data_throughput_events_per_sec Current fanout throughput
# TYPE market_data_throughput_events_per_sec gauge
market_data_throughput_events_per_sec 376.1

# HELP market_data_client_sent_total Total messages sent per client
# TYPE market_data_client_sent_total counter
market_data_client_sent_total{client_id="f023ce3e"} 1200

# HELP market_data_client_dropped_total Total messages dropped per client
# TYPE market_data_client_dropped_total counter
market_data_client_dropped_total{client_id="f023ce3e"} 0

# HELP market_data_client_latency_p50_ms Rolling p50 dispatch latency in ms
# TYPE market_data_client_latency_p50_ms gauge
market_data_client_latency_p50_ms{client_id="f023ce3e"} 8.12

# HELP market_data_client_latency_p99_ms Rolling p99 dispatch latency in ms
# TYPE market_data_client_latency_p99_ms gauge
market_data_client_latency_p99_ms{client_id="f023ce3e"} 11.46
```

Latency series are omitted for clients with zero latency samples. Implementation generates text via `_prometheus_metrics_text()` helper; no `prometheus-client` library dependency.

**GET /history/{symbol}**

Returns stored ticks for a symbol in a given `event_ts` range. Filters by `event_time` (derived from `event_ts`), ordered ascending.

```
Request:  GET /history/AAPL?from_ts=1710001200000&to_ts=1710004800000&limit=100
Response: 200
{
  "symbol": "AAPL",
  "from_ts": 1710001200000,
  "to_ts":   1710004800000,
  "count": 1,
  "ticks": [
    {
      "seq": 101,
      "event_ts": 1710001234567,
      "bid": 189.10,
      "ask": 189.12,
      "bid_size": 500,
      "ask_size": 300
    }
  ]
}

Response: 400 — missing/invalid params, from_ts > to_ts, limit <= 0
{"error": "from_ts and to_ts required"}

Response: 404 — no ticks in range
{"error": "No history for AAPL"}

Response: 503 — history database unavailable
Response: 504 — database query timeout
{"error": "history query timed out"}
```

Query params:
- `from_ts`, `to_ts` — required, integer ms epoch
- `limit` — optional, default 1000, max 10000

**GET /metrics/summary**

Aggregated stats across all connected clients.

```
{
  "clients": 3,
  "overall_p50_ms": 8.33,
  "overall_p99_ms": 10.22,
  "average_client_p50_ms": 8.10,
  "average_client_p99_ms": 10.05,
  "latency_samples": 1000,
  "total_sent": 25687,
  "total_dropped": 0
}
```

### 7.2 WebSocket Protocol

**Endpoint:** `ws://localhost:8000/stream`
**Query params:**
- `mode=raw` (default) | `mode=agg_100ms`
- `encoding=json` (default, text frames) | `encoding=msgpack` (binary frames); unknown values fall back to `json`

**Client → Server:**

```json
{"action": "subscribe",   "symbol": "AAPL"}
{"action": "unsubscribe", "symbol": "AAPL"}
```

**Server → Client (snapshot, sent immediately on subscribe):**

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

**Server → Client (incremental update):**

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

## 8. Performance Characteristics

### 8.1 Dispatch Latency

Validated 2026-09-12: 50 events/sec per symbol (250 total), 5 symbols,
2 subscribed per client, 15s duration. Percentiles use combined client-observed
latency samples and every scenario validates received frames:

| Clients | p50 (ms) | p99 (ms) | Received | Dropped |
|---------|----------|----------|------------|---------|
| 1       | 8.72     | 10.86    | 1,474      | 0       |
| 5       | 8.96     | 11.12    | 7,424      | 0       |
| 10      | 9.22     | 11.92    | 14,842     | 0       |
| 20      | 9.90     | 13.03    | 29,655     | 0       |

**Key finding:** 20-client p99 remains below 15ms with zero drops. Per-client
queue isolation satisfies the measured normal-load target.

### 8.2 Serialization

100,000 iterations per format:

| Format  | Payload | Encode       | Decode       | Total |
|---------|---------|-------------|-------------|-------|
| JSON    | 204 B   | 590k ops/s  | 557k ops/s  | 349ms |
| msgpack | 151 B   | 1,710k ops/s| 1,627k ops/s| 120ms |

msgpack: **2.91x faster, 26% smaller payload.**

Clients connecting with `?encoding=msgpack` receive binary WebSocket frames encoded via `msgpack.packb(use_bin_type=True)`. JSON clients are unaffected. Benchmark-verified payload reduction: 25.8% on a standard MarketEvent dict.

---

## 9. Project Structure

```
.
├── docker-compose.yml                  # Kafka + Zookeeper + Redis + TimescaleDB + Kafka UI
├── requirements.txt                    # Python dependencies
├── pytest.ini                          # Test configuration
├── README.md                           # Architecture documentation
├── .github/workflows/ci.yml           # GitHub Actions: pytest on every push
├── src/
│   ├── models.py                       # MarketEvent, SnapshotData
│   ├── feed/
│   │   └── simulator.py                # FeedSimulator → Kafka producer
│   ├── engine/
│   │   ├── snapshot_store.py           # Redis HSET read/write + Pub/Sub publish
│   │   └── engine.py                   # KafkaConsumerBridge + DistributionEngine (separate process)
│   ├── gateway/
│   │   ├── session.py                  # ClientSession, BoundedQueue, LatencyTracker
│   │   ├── aggregator.py              # AggregationBuffer (RAW / AGG_100MS)
│   │   └── gateway.py                 # FastAPI WebSocket + REST gateway
│   ├── storage/
│   │   ├── history_store.py            # asyncpg pool, schema init, bulk INSERT, range query
│   │   └── tick_writer.py              # Kafka consumer (tick-storage group), batch writer
│   └── benchmark/
│       ├── serialization_bench.py     # JSON vs msgpack
│       ├── load_bench.py              # Multi-client latency benchmark
│       └── results.md                 # Raw benchmark output
└── tests/
    ├── conftest.py                     # Shared fixtures
    ├── test_session.py                 # ClientSession unit tests
    ├── test_aggregator.py             # AggregationBuffer unit tests
    ├── test_snapshot_store.py         # SnapshotStore unit tests (Redis mocked)
    ├── test_models.py                 # MarketEvent model tests
    ├── test_engine.py                 # DistributionEngine unit tests (Pub/Sub publish verified)
    ├── test_history_store.py          # HistoryStore unit tests (asyncpg mocked)
    └── test_tick_writer.py            # TickWriter unit tests (store + consumer mocked)
```

---

## 10. Infrastructure

### 10.1 Docker Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| kafka | confluentinc/cp-kafka:7.6.0 | 9092 | Message broker |
| zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 | Kafka coordination |
| redis | redis:7.2-alpine | 6379 | SnapshotStore, AOF enabled |
| timescaledb | timescale/timescaledb:2.14.2-pg16 | 5432 | Tick storage |
| kafka-ui | provectuslabs/kafka-ui:latest | 8080 | Debugging UI |

The load balancer is a deployment-level component for multi-node gateway runs. The local `docker-compose.yml` currently defines shared infrastructure services only; gateway and nginx processes are started separately for local development.

### 10.2 Python Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| fastapi | 0.111.0 | WebSocket + REST gateway |
| uvicorn | 0.29.0 | ASGI server |
| confluent-kafka | 2.4.0 | Kafka producer/consumer |
| redis | 5.0.4 | Async Redis client |
| websockets | 12.0 | WebSocket client (testing) |
| msgpack | 1.0.8 | Binary serialization (benchmarked) |
| aiohttp | 3.13.5 | HTTP client (load benchmark) |
| asyncpg | 0.29.0 | Async PostgreSQL/TimescaleDB client |

---

## 11. Future Extensions

| Extension | Description | Complexity | Status |
|-----------|-------------|------------|--------|
| Multi-node gateway | Redis Pub/Sub for cross-instance fanout, load balancer | Medium | **Done** |
| Kafka offset replay | Small-gap reconnect replays from Kafka instead of snapshot | Medium | Planned |
| Historical storage | Dedicated consumer group writes ticks to TimescaleDB async | Medium | **Done** |
| Conditional subscriptions | Filter by spread, price change threshold | Low | **Done** |
| TLS/auth | mTLS for WebSocket, API key for REST | Medium | Planned |
| msgpack wire format | Replace JSON end-to-end for 26% bandwidth reduction | Low | **Done** |
| Prometheus export | Expose /metrics in Prometheus format, Grafana dashboards | Low | **Done** |
