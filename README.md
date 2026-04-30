# Real-Time Market Data Platform

A high-throughput market data distribution system that fans out real-time price updates to WebSocket subscribers with bounded latency and controlled backpressure.

## Architecture

```
┌─────────────────┐
│  FeedSimulator  │  Mock price generator (random walk, 50 events/sec)
└────────┬────────┘
         │ produce (key=symbol)
         ▼
┌─────────────────┐
│      Kafka      │  topic: market-events, partitioned by symbol
└────────┬────────┘  guarantees per-symbol ordering
         │ consume (dedicated thread)
         ▼
┌──────────────────────────────────────────┐
│          DistributionEngine              │
│                                          │
│  KafkaConsumerBridge                     │
│  (thread → asyncio bridge)               │
│           │                              │
│           ▼                              │
│  AggregationBuffer  ←── raw / 100ms mode │
│           │                              │
│  FanoutDispatcher                        │
│  symbol → ClientSession set             │
└──────────┬───────────────────────────────┘
           │ non-blocking enqueue
           ▼
┌──────────────────────────────────────────┐
│         ClientSession (per client)       │
│                                          │
│  BoundedQueue (maxsize=500)              │
│  SlowConsumerPolicy: drop_oldest         │
│  LatencyTracker: p50 / p99              │
│  CoalescingBuffer: per-symbol            │
└──────────┬───────────────────────────────┘
           │ WebSocket push
           ▼
        Clients

Redis: SnapshotStore (HSET snapshot:{symbol})
       Read on new client connect → snapshot + seq alignment
```

## Key Design Decisions

### 1. Snapshot + Incremental Update

New clients cannot receive only incremental updates — they have no prior context.

**Flow:**
1. Client connects via WebSocket
2. Client sends `{"action": "subscribe", "symbol": "AAPL"}`
3. Server reads current snapshot from Redis (includes `seq=N`)
4. Server sends snapshot to client
5. Client receives only incremental updates with `seq > N`
6. Client detects gap (seq jump > 5) → requests re-snapshot

This ensures no missed updates and no duplicate data on reconnect.

### 2. Kafka Partition by Symbol

```
symbol hash → partition
```

- AAPL always lands in the same partition → strict ordering guaranteed
- Different symbols processed in parallel across partitions
- Enables horizontal scaling: add consumers = handle more symbols

### 3. asyncio + Kafka Thread Bridge

`confluent-kafka` is a blocking synchronous API. Running it directly in asyncio would block the entire event loop.

**Solution:** Kafka consumer runs in a dedicated thread. Events are bridged into the asyncio event loop via `asyncio.run_coroutine_threadsafe`.

### 4. Per-Client BoundedQueue

Each client has an independent outbound queue (`asyncio.Queue(maxsize=500)`).

**Why this matters:** In a naive implementation, a slow client blocks `await ws.send_text()`, which blocks the entire fanout loop, which delays all other clients.

With per-client queues:
- Fanout loop only does `queue.put_nowait()` — never blocks
- Each client has an independent writer coroutine
- Slow clients are isolated

### 5. Slow Consumer Policy: Drop Oldest

When a client's outbound queue is full:
- Discard the **oldest** pending message
- Enqueue the **latest** message

Rationale: in market data, old quotes expire immediately. A client that is 500 messages behind is better served by the latest price than a stale one from seconds ago.

### 6. Coalescing

If multiple updates for the same symbol are pending in a client's queue, they are coalesced into the single latest update.

This prevents queue buildup during burst traffic while ensuring the client always sees the most recent state.

## Performance Results

Single client, 2 symbols (AAPL + TSLA), 50 events/sec:

| Metric | Value |
|--------|-------|
| p50 dispatch latency | ~8ms |
| p99 dispatch latency | ~11ms |
| Throughput (15s) | ~2000 messages |
| Dropped messages | 0 |

Serialization (100,000 iterations):

| Format | Payload | Encode | Decode |
|--------|---------|--------|--------|
| JSON | ~180 bytes | baseline | baseline |
| msgpack | ~120 bytes | ~2x faster | ~2x faster |

## Failure Handling

| Scenario | Behavior |
|----------|----------|
| Client disconnect | Removed from all subscription registries, writer task cancelled |
| Slow consumer | Drop oldest messages, disconnect if drop count exceeds threshold |
| Kafka consumer lag | Bounded internal queue (10,000), backpressure to consumer thread |
| Redis restart | AOF persistence, snapshot restored on startup; Kafka replay fills gaps |
| Seq gap detected | Client requests re-snapshot via subscribe action |

## API

### REST

```
GET /snapshot/{symbol}     Current snapshot for a symbol
GET /metrics               Per-client stats (sent, dropped, coalesced, p50/p99)
GET /metrics/summary       Aggregated stats across all clients
```

### WebSocket

```
ws://localhost:8000/stream
ws://localhost:8000/stream?mode=agg_100ms   # 100ms aggregated mode
```

**Subscribe:**
```json
{"action": "subscribe", "symbol": "AAPL"}
```

**Unsubscribe:**
```json
{"action": "unsubscribe", "symbol": "AAPL"}
```

**Snapshot response (on subscribe):**
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

**Incremental update:**
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

## Running Locally

```bash
# Start infrastructure
docker compose up -d

# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Terminal 1: Feed simulator
python3 -m src.feed.simulator

# Terminal 2: Distribution engine + gateway
python3 -m src.gateway.gateway

# Verify
curl http://localhost:8000/snapshot/AAPL
curl http://localhost:8000/metrics/summary
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| WebSocket / REST | FastAPI + uvicorn |
| Message broker | Apache Kafka (confluent-kafka) |
| Snapshot store | Redis (redis-py async) |
| Concurrency | asyncio + threading bridge |
| Serialization | JSON (msgpack available) |
| Infrastructure | Docker Compose |

## Future Extensions

- **Multi-node gateway**: multiple WebSocket gateway instances sharing Redis pub/sub for cross-process fanout
- **Kafka replay on reconnect**: clients with small seq gap replay from Kafka offset instead of re-fetching snapshot
- **Historical storage**: dedicated `history-writer` consumer group writes ticks to PostgreSQL asynchronously, isolated from hot path
- **True msgpack wire format**: replace JSON encoding end-to-end for ~30% payload reduction
