---
name: market-review
description: Review code changes in this real-time market data project. Use when Codex submits an implementation, when reviewing a PR, or when asked to audit new code against the project's architecture. Checks asyncio correctness, per-client isolation, sequence number handling, Redis patterns, WebSocket lifecycle, and performance against the invariants in SPEC.md.
---

You are reviewing code changes for the **Real-Time Market Data Platform**. Your job is to catch bugs, architectural violations, and performance regressions — not to suggest style improvements or refactors beyond the stated scope.

## Review procedure

1. Read the diff (or the relevant changed files).
2. Run each checklist below. For every finding, note: **file:line**, **severity** (Critical / Major / Minor), **what's wrong**, and **what the correct behavior should be**.
3. Summarize findings at the end grouped by severity.

Do not approve code with any Critical findings.

---

## Checklist 1 — asyncio correctness

These violations cause event loop stalls and latency spikes.

- [ ] No blocking I/O in the event loop: no `time.sleep()`, no synchronous file/network calls, no `consumer.poll()` called directly from a coroutine. Blocking work must live in `KafkaConsumerBridge._consume_loop` (thread) or behind `asyncio.run_in_executor`.
- [ ] All `asyncio.Task` coroutines handle `asyncio.CancelledError` — either let it propagate or re-raise after cleanup. Swallowing it with a bare `except Exception` is a bug.
- [ ] `asyncio.run_coroutine_threadsafe` is the only way to cross the thread/asyncio boundary from `KafkaConsumerBridge._consume_loop`. Never call `loop.call_soon()` or queue methods directly from the consumer thread.
- [ ] Tasks are awaited or cancelled on shutdown — no orphaned tasks.
- [ ] `await asyncio.sleep(0)` is used (not `time.sleep(0)`) when yielding within a loop.

## Checklist 2 — per-client isolation

These violations allow one client to affect another's performance.

- [ ] The fanout loop (`fanout_loop` in `gateway.py`) must never `await` anything per-client that could block on slow clients. Specifically: no `await session.aggregator.push(event)` that can block — `push` must be non-blocking from the fanout's perspective.
- [ ] All outbound writes to WebSocket go through `session.enqueue()` → `_queue` → `_writer_loop`. Direct `await websocket.send_text()` from the fanout loop or dispatch loop is a critical isolation violation.
- [ ] Shared state between sessions (the `subscriptions` dict, `all_sessions`) must be accessed under `subscriptions_lock`. New code that reads or mutates these dicts without the lock is a race condition.
- [ ] New per-client state must live in `ClientSession`, not in module-level globals. Module-level globals that grow with connected clients will leak memory.

## Checklist 3 — sequence numbers and gap detection

- [ ] Any new event type that flows through the distribution pipeline must carry a `seq` field. Events without `seq` break gap detection in RAW mode.
- [ ] `check_gap` tolerance is 5 (per `session.py:120`). If new code changes this constant or bypasses `check_gap` in RAW mode, it must justify why.
- [ ] On gap detection, the recovery path must: fetch fresh snapshot from Redis, call `session.enqueue({"type": "snapshot", ...})`, update `session.last_seq[symbol]`, and `continue` (skip the gapped event). Missing any of these steps leaves the client in an inconsistent state.
- [ ] In AGG_100MS mode, `check_gap` must NOT be called — seq jumps are expected. New code must not accidentally enable gap detection for aggregated clients.
- [ ] Snapshot delivery on subscribe (`_subscribe` in `gateway.py`) seeds `last_seq[symbol] = snapshot.seq`. Any new subscription path must also seed `last_seq`.

## Checklist 4 — Redis / SnapshotStore

- [ ] All snapshot reads and writes go through `SnapshotStore`, not through a raw `redis.Redis` or `aioredis` client. Direct Redis calls bypass TTL management and the `SnapshotData` schema.
- [ ] TTL is 86400 seconds. Any code that writes to Redis must preserve this TTL.
- [ ] `SnapshotStore.update(event)` uses `HSET` (atomic across all fields). Writes that set fields individually (multiple SET calls) are not atomic and can produce partial snapshots.
- [ ] `await snapshot_store.close()` is called on shutdown. New code that creates additional `SnapshotStore` instances must also close them.

## Checklist 5 — WebSocket lifecycle and cleanup

- [ ] On client disconnect (whether clean or error), the cleanup sequence must: cancel dispatch task, call `session.aggregator.stop()`, call `_cleanup(session)` (removes from subscriptions + all_sessions, calls `session.close()`). Missing any step leaks tasks, registry entries, or Redis subscriptions.
- [ ] `_cleanup` removes the session from all symbols in `session.subscriptions`. New code that adds symbols to `session.subscriptions` must use the same set (not a shadow copy).
- [ ] `session.close()` cancels `_writer_task` and closes the WebSocket. New cleanup paths must not close the WebSocket before the writer task is cancelled (writer task will throw on next send, but that's fine — it must not deadlock).
- [ ] New REST endpoints that read `all_sessions` must do so under `subscriptions_lock` to avoid seeing a partially-removed session.

## Checklist 6 — performance hot path

The non-functional target is p99 < 15ms under 20 clients (NFR-1).

- [ ] `fanout_loop` must be O(subscribers) for event delivery, and O(1) per subscriber. No per-event Redis reads inside the fanout loop (Redis writes for snapshot update are acceptable).
- [ ] No sorting, filtering, or regex inside the hot path (fanout_loop, client_dispatch_loop, AggregationBuffer flush loop) unless it was there before.
- [ ] New `asyncio.Lock` acquisitions inside the fanout loop: flag any lock that is held while awaiting I/O. The `subscriptions_lock` scope must be minimal — grab a copy of the set, release the lock, then iterate.
- [ ] `AggregationBuffer.push()` is called inside the fanout loop and must be non-blocking. Any change to `push()` that adds `await` must be flagged.

## Checklist 7 — tests

- [ ] New features have tests in `tests/`. No new public method or behavior is exempt.
- [ ] Tests use the existing fixture patterns from `tests/conftest.py` (mock WebSocket, mock Redis). New external dependencies (new services, new ports) must be mockable.
- [ ] Tests for async code use `pytest-asyncio` (`@pytest.mark.asyncio`), not `asyncio.run()`.
- [ ] Tests do not `time.sleep()` to wait for async operations — use `asyncio.sleep(0)` to yield, or `asyncio.wait_for` with a timeout.
- [ ] Gap detection tests cover: no gap (normal), gap exactly at threshold (seq + 5), gap above threshold (seq + 6), and AGG_100MS mode (no gap detection).

---

## Output format

```
## Review: <feature or PR title>

### Critical
- `file.py:line` — <what's wrong and why it's a critical bug>

### Major  
- `file.py:line` — <what's wrong>

### Minor
- `file.py:line` — <what's wrong>

### Verdict
APPROVE / REQUEST CHANGES — <one sentence summary>
```

If there are no findings in a severity category, omit it. If the diff is clean across all checklists, say so explicitly.
