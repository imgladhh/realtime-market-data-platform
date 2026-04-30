# Benchmark Results

All benchmarks run on a local development machine (Windows 11, WSL2 Ubuntu 24, Intel i7, 32GB RAM).

Infrastructure: Docker Desktop with Kafka 7.6.0, Redis 7.2, Python 3.12, venv.

---

## 1. Serialization Benchmark

Compares JSON vs msgpack for encoding and decoding a single `MarketEvent`.

**Command:**
```bash
python3 -m src.benchmark.serialization_bench
```

**Sample event:**
```python
{
  "symbol": "AAPL", "bid": 189.11, "ask": 189.13,
  "bid_size": 500, "ask_size": 300,
  "event_ts": 1710001234567, "server_ts": 1710001234571,
  "seq": 102938, "type": "quote", "last_price": 0.0, "last_size": 0
}
```

**Results (100,000 iterations):**

| Format  | Payload (bytes) | Encode (ops/sec) | Decode (ops/sec) | Total (ms) |
|---------|----------------|------------------|------------------|------------|
| JSON    | 204            | 590,130          | 556,954          | 349.0      |
| msgpack | 151            | 1,710,868        | 1,627,463        | 119.9      |

**Summary:**
- Payload size reduction: **26.0%**
- Speed improvement: **2.91x faster** (encode + decode combined)

**Takeaway:**
At 50 events/sec per symbol across 5 symbols, msgpack saves ~1.3KB/sec per client on the wire.
At 10,000 events/sec (realistic production load), the serialization throughput difference becomes significant:
JSON saturates at ~570k ops/sec; msgpack handles ~1.6M ops/sec with headroom to spare.

---

## 2. Dispatch Latency Benchmark

Measures end-to-end dispatch latency (time from `event_ts` on the MarketEvent to when it is written to the WebSocket) under increasing concurrent client load.

**Setup:**
- Feed: 50 events/sec, 5 symbols (AAPL, TSLA, GOOGL, MSFT, BTCUSD)
- Each client subscribes to AAPL + TSLA
- Duration: 15 seconds per scenario
- Aggregation mode: RAW (every tick delivered)
- Metrics sampled while clients are connected

**Command:**
```bash
python3 -m src.benchmark.load_bench
```

**Results:**

| Clients | p50 (ms) | p99 (ms) | Total Sent | Dropped |
|---------|----------|----------|------------|---------|
| 1       | 8.33     | 11.25    | 1,274      | 0       |
| 5       | 7.85     | 9.40     | 6,429      | 0       |
| 10      | 7.99     | 9.54     | 12,838     | 0       |
| 20      | 8.38     | 10.22    | 25,687     | 0       |

**Key observations:**

1. **Latency is stable across client counts.**
   p99 stays within 11.25ms → 9.40ms → 9.54ms → 10.22ms as clients scale from 1 to 20.
   This validates the per-client BoundedQueue design: the fanout loop is never blocked by a slow client,
   so adding more clients does not increase tail latency for existing ones.

2. **Zero drops at all client counts.**
   No messages were dropped even at 20 concurrent clients, each receiving 2 symbol streams.
   The outbound queue (maxsize=500) was never saturated in this scenario.

3. **Linear throughput scaling.**
   Total messages sent scales linearly: 1,274 → 6,429 → 12,838 → 25,687,
   roughly proportional to client count as expected.

---

## 3. Aggregation Mode Comparison

Demonstrates the difference between RAW and AGG_100MS delivery modes.

**RAW mode** (default):
```
ws://localhost:8000/stream
```
Every tick delivered immediately. Lowest latency.

```
type=snapshot seq=188381 bid=207.37
type=quote    seq=188401 bid=207.91   ← ~20 seq gap = ~100ms, 5 symbols
type=quote    seq=188426 bid=207.88
type=quote    seq=188451 bid=207.65
...
```

**AGG_100MS mode:**
```
ws://localhost:8000/stream?mode=agg_100ms
```
Events buffered per symbol in 100ms windows. Only latest event per symbol per window is emitted.

```
type=snapshot seq=188381 bid=207.37
type=quote    seq=188401 bid=207.91   ← only latest AAPL in each 100ms window
type=quote    seq=188426 bid=207.88
...
```

**Tradeoff:**

| Mode      | Latency      | Bandwidth | Use case                        |
|-----------|--------------|-----------|---------------------------------|
| RAW       | Lowest (~8ms p50) | Higher | Professional / algo trading clients |
| AGG_100MS | +100ms window | ~60-80% lower | Retail / dashboard clients |

---

## Reproducing Results

```bash
# Start infrastructure
docker compose up -d

# Activate environment
source venv/bin/activate

# Terminal 1: simulator
python3 -m src.feed.simulator

# Terminal 2: gateway
python3 -m src.gateway.gateway

# Terminal 3: run benchmarks
python3 -m src.benchmark.serialization_bench
python3 -m src.benchmark.load_bench
```

Note: latency numbers include WSL2 loopback overhead (~2-5ms).
On a native Linux machine or in production (co-located services), expect lower absolute numbers
with the same relative scaling behavior.