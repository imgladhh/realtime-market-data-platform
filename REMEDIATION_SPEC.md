# Real-Time Market Data Processor — Correctness Remediation Specification

Status: P0 implemented; P1 and P2 proposed  
Created: 2026-09-11  
Scope: Teaching and portfolio project  
Baseline: 94 unit tests passing before remediation  
P0 validation: 110 unit tests passing on 2026-09-11

## 1. Purpose

This specification converts the repository review findings into an ordered implementation plan. The goal is to make the behavior claimed by `SPEC.md` correct and demonstrable without expanding the project into a full production trading platform.

The implementation order is binding:

1. P0 — prevent permanent data loss and stale state delivery.
2. P1 — close recovery races and make advertised timing and benchmark behavior true.
3. P2 — align secondary API behavior, tests, and documentation.

No P1 work should delay a safe, independently testable P0 fix.

## 2. Guiding Principles

- Kafka offsets are acknowledgements of completed downstream work, not acknowledgements of polling.
- A database timeout is an unknown result, not proof that a write failed.
- Snapshot delivery establishes a per-symbol sequence boundary. No event at or below that boundary may be delivered afterward.
- RAW mode aims to preserve and validate every per-symbol update. AGG_100MS intentionally coalesces updates.
- Redis Pub/Sub is an ephemeral notification layer. Redis snapshots are the current-state recovery source.
- Fanout and per-client routing must remain non-blocking.
- Tests must validate failure windows, not only successful calls.

## 3. Explicit Non-Goals

The following production-level additions are not required by this remediation:

- TLS, authentication, authorization, quotas, or tenant isolation.
- Redis Cluster, Redis Sentinel, Kafka multi-broker failover, or cross-region operation.
- Active-active DistributionEngine leader election.
- Lossless WebSocket replay with client ACKs and resumable offsets.
- Kubernetes deployment, autoscaling, or full observability infrastructure.
- Replacing Redis Pub/Sub with Redis Streams or another durable broadcast system.
- Supporting arbitrary symbol universes or exchange-specific market-data protocols.

These may remain documented as future work. They must not be presented as implemented behavior.

## 4. Priority Summary

| Order | ID | Priority | Area | Required outcome |
|---:|---|:---:|---|---|
| 1 | REM-P0-01 | P0 | TickWriter | A timed-out database write never causes its Kafka offsets to be committed. |
| 2 | REM-P0-02 | P0 | TickWriter | A malformed record cannot commit past valid records still held in memory. |
| 3 | REM-P0-03 | P0 | DistributionEngine | Kafka offsets are committed only after Redis snapshot update and publish succeed. |
| 4 | REM-P0-04 | P0 | Sequencing | Duplicate or stale events are never delivered after a newer snapshot/event. |
| 5 | REM-P0-05 | P0 | Sequence contract | RAW gap detection uses a true per-symbol monotonic sequence. |
| 6 | REM-P1-01 | P1 | Subscribe flow | Snapshot and incremental activation have no unobserved delivery window. |
| 7 | REM-P1-02 | P1 | TickWriter | The 100 ms batch deadline is enforced during continuous traffic. |
| 8 | REM-P1-03 | P1 | Redis Pub/Sub | A failed channel subscription cannot leave a silent, permanently inactive symbol. |
| 9 | REM-P1-04 | P1 | Metrics/benchmark | Reported dispatch latency and delivery counts represent successful client delivery. |
| 10 | REM-P2-01 | P2 | History API | Database unavailability follows the documented error contract. |
| 11 | REM-P2-02 | P2 | Input validation | Conditional subscription thresholds reject invalid numeric values. |
| 12 | REM-P2-03 | P2 | Documentation | README, SPEC, tests, and benchmark claims describe the implemented system. |
| 13 | REM-P1-05 | P1 | DistributionEngine | Transient Redis failures do not permanently terminate event processing. |
| 14 | REM-P2-04 | P2 | DistributionEngine | Kafka acknowledgements are coalesced per partition without weakening commit safety. |
| 15 | REM-P2-05 | P2 | TickWriter shutdown | The intentional replay of a partial tail batch is explicit and contains no misleading dead flush path. |
| 16 | REM-P2-06 | P2 | Metrics cleanup | The unused `dropped_ticks` counter is removed or given an explicit, reachable meaning. |

## 5. P0 Requirements

### REM-P0-01 — Do not drop and commit timed-out tick batches

#### Problem

`TickWriter._flush()` currently treats `TimeoutError` as a permanent drop, increments `dropped_ticks`, and commits the consumer position. A timeout leaves the database result unknown. Committing can permanently lose ticks and violates FR-12.

#### Required behavior

- `TimeoutError` and `asyncio.TimeoutError` must not commit Kafka offsets.
- A timed-out batch must remain the current batch and be retried with bounded exponential backoff.
- Retry safety must rely on the existing idempotent `ON CONFLICT DO NOTHING` insert.
- If shutdown occurs while the batch is unresolved, the writer must stop without committing it so Kafka can replay it.
- `dropped_ticks` must not be incremented for a retriable timeout. If the field remains, its meaning must be explicitly limited to an intentional, terminal discard policy; no such policy is required here.

#### Acceptance criteria

- A timeout followed by success produces at least two insert attempts and exactly one commit.
- Repeated timeouts followed by shutdown produce zero commits.
- Replaying a batch whose first attempt may have reached TimescaleDB does not create duplicate rows.
- The old test expecting timeout-plus-commit is replaced.

#### Required tests

- `test_flush_retries_timeout_without_commit_until_success`
- `test_flush_shutdown_during_timeout_retry_does_not_commit`
- Existing `HistoryStore` idempotency assertion remains passing.

### REM-P0-02 — Prevent poison-message commits from skipping buffered data

#### Problem

When parsing fails, `TickWriter.run()` commits the malformed Kafka message immediately. If earlier valid records from the same partition are still in the in-memory batch, the commit advances past records that have not been written.

#### Required behavior

- Before committing or skipping a malformed message, all earlier buffered events must be flushed successfully.
- If that flush fails or shutdown begins, the malformed message must not be committed.
- After the preceding batch succeeds, the malformed message may be synchronously committed and logged as skipped.
- A DLQ is optional and outside the required scope.

#### Acceptance criteria

- Given valid offsets 10 and 11 followed by malformed offset 12 in one partition, offset 12 is not committed until 10 and 11 are persisted.
- A failure while flushing 10 and 11 results in no commit for offset 12.
- A malformed message with an empty batch may be skipped and committed immediately.

#### Required tests

- Add a partition-aware fake consumer/message test for valid-valid-malformed ordering.
- Assert call order: insert valid batch, commit batch, then commit malformed message.

### REM-P0-03 — Commit live-distribution offsets after Redis processing

#### Problem

`KafkaConsumerBridge` enables Kafka auto-commit. The consumer thread may commit a message while it is still waiting in the asyncio queue, before `SnapshotStore.update()` and `publish_event()` run. The ignored `run_coroutine_threadsafe()` futures also allow pending `queue.put()` coroutines to grow beyond the nominal queue bound.

#### Required behavior

- `enable.auto.commit` must be `False` for the `gateway-engine` consumer.
- A consumed item passed to asyncio must preserve enough Kafka metadata to acknowledge its partition and offset.
- The consumer object must remain owned and called by its polling thread.
- Async processing must send a success acknowledgement back to the consumer thread only after both snapshot update and Pub/Sub publish succeed.
- Only acknowledged offsets may be committed.
- A Redis failure must retain the item for retry or leave it uncommitted before process termination.
- The bridge must bound total in-flight work. A full asyncio queue must slow or pause Kafka polling rather than create an unbounded number of pending coroutine futures.
- Shutdown must stop polling, resolve or abandon outstanding work without committing it, close the consumer, and join the thread.

#### Suggested implementation shape

Use an envelope containing `MarketEvent`, topic, partition, and offset. Keep a bounded handoff queue plus a thread-safe acknowledgement path. The Kafka thread drains acknowledgements and performs commits. The exact class layout is not prescribed, but cross-thread calls to the Kafka consumer are prohibited.

#### Acceptance criteria

- Polling alone never commits.
- Successful Redis update plus publish causes the corresponding offset to be committed.
- Snapshot failure and publish failure both produce zero commit for the failed event.
- Queue saturation cannot create more pending handoffs than the configured bound.
- Engine shutdown leaves no live consumer thread.

#### Required tests

- Unit tests for success, snapshot failure, publish failure, queue saturation, and shutdown.
- One optional Docker integration test verifies restart replays an event that was polled but not acknowledged.

### REM-P0-04 — Reject duplicate and stale incrementals

#### Problem

`ClientSession.check_gap()` treats only a large forward jump as exceptional. It always writes `last_seq = seq`, including when `seq <= last_seq`. After gap recovery, buffered older events can therefore be sent after a newer snapshot and can move the sequence baseline backward.

#### Required behavior

RAW dispatch must classify each event as one of:

- `NEXT`: the expected next per-symbol sequence; deliver and advance the baseline.
- `GAP`: sequence is ahead of the expected next value; fetch a fresh snapshot and do not deliver the triggering event.
- `STALE`: sequence is equal to or behind the current boundary; discard it and do not change the baseline.

A boolean return value is not sufficient to represent all three states. Use an enum or equivalent explicit result.

After a recovery snapshot at sequence N:

- Any queued event with sequence `<= N` must be discarded.
- The next deliverable RAW event must be sequence `N + 1` under the per-symbol sequence contract.
- A failed snapshot enqueue must not silently advance the session boundary.

#### Acceptance criteria

- Snapshot 205 followed by buffered events 201–205 never sends those events.
- A duplicate event does not alter `last_seq`, `last_price`, sent counts, or filter baselines.
- A forward gap sends one recovery snapshot and suppresses the triggering stale incremental.
- AGG_100MS continues to allow sequence jumps by design, but must not deliver an event older than its most recent delivered snapshot/event.

#### Required tests

- `test_stale_event_after_recovery_snapshot_is_discarded`
- `test_duplicate_event_does_not_regress_sequence`
- `test_gap_recovery_does_not_advance_when_snapshot_enqueue_fails`

### REM-P0-05 — Make sequence numbers per-symbol

#### Problem

The current global sequence happens to advance by five for each symbol because the simulator emits five symbols in a fixed round-robin. The gap tolerance of five therefore encodes simulator topology rather than a valid per-symbol ordering contract.

#### Decision

`MarketEvent.seq` will become a monotonically increasing sequence within each symbol. Global cross-symbol ordering is not required by the product and must not be inferred from `seq`.

#### Required behavior

- `FeedSimulator` maintains one counter per symbol.
- The first event for each symbol uses sequence 1 and increments by one for later events of that symbol.
- Kafka remains keyed by symbol, preserving the per-symbol order.
- Redis snapshots store the per-symbol sequence unchanged.
- RAW gap detection expects exactly `last_seq + 1`.
- History deduplication remains based on symbol, sequence, and event time; a schema change is not required.
- Documentation and sample payloads must stop calling `seq` globally monotonic.

#### Acceptance criteria

- Interleaving AAPL and TSLA produces independent sequences `AAPL: 1,2,3` and `TSLA: 1,2,3`.
- Missing AAPL sequence 2 is detected when AAPL sequence 3 arrives, regardless of activity in other symbols.
- Existing snapshot and history conversions preserve the new semantics.

## 6. P1 Requirements

### REM-P1-01 — Close the snapshot-to-subscription race

#### Problem

The current subscribe flow reads and enqueues a snapshot, registers the local session, and only then subscribes the gateway to the Redis channel. Events published between these steps may be missed. Simply reversing two calls can instead deliver an incremental before the snapshot.

#### Required behavior

- A symbol subscription must have an explicit initialization state.
- The Redis channel must be confirmed active before the client subscription is reported as active.
- Events observed while the session is initializing must be buffered per session and symbol, not sent directly.
- The gateway then reads and enqueues the snapshot, seeds the sequence boundary, discards buffered events at or below the snapshot sequence, and releases newer events in order.
- The client must always observe snapshot first, then incrementals with greater sequences.
- If channel setup or snapshot retrieval fails, the session must be rolled back to unsubscribed state and receive an explicit error message.
- Initialization buffers must be bounded and cleaned up on disconnect or unsubscribe.

#### Acceptance criteria

- An event published during snapshot retrieval is either represented by the snapshot or delivered afterward; it is never silently lost.
- No incremental is sent before the snapshot.
- Concurrent first subscribers cause one Redis channel subscription while both sessions initialize correctly.
- Disconnect during initialization leaves no session, task, channel, or buffer leak.

#### Required tests

- Deterministic tests using events/barriers rather than sleeps for each race boundary.
- A test where snapshot sequence N overlaps buffered sequences N and N+1.

### REM-P1-02 — Enforce batch latency during continuous traffic

#### Problem

TickWriter checks the 100 ms deadline only when `consumer.poll()` returns no message. Under continuous traffic, a partial batch can remain open until it reaches 500 events.

#### Required behavior

- Check both size and elapsed time after every event is appended.
- Poll timeout should not exceed the remaining batch deadline when a batch is open.
- The flush condition is: maximum size reached or maximum latency reached, whichever occurs first.

#### Acceptance criteria

- Continuous traffic below the size threshold flushes within the configured deadline plus a small scheduling tolerance.
- High-volume traffic still flushes immediately at the size threshold.
- Tests use a fake clock or controlled monotonic values where practical.

### REM-P1-03 — Make Pub/Sub subscription failure recoverable

#### Problem

The gateway adds a channel to `active_channels` before Redis confirms `SUBSCRIBE`. If the call fails, later subscriptions see the channel as active and return without retrying.

#### Required behavior

- Separate desired channels from confirmed subscribed channels, or otherwise represent both states explicitly.
- A channel is confirmed only after Redis subscribe succeeds.
- A failed subscribe must trigger bounded retry or fail and roll back the initiating client subscription.
- Later clients must be able to trigger recovery; they must not inherit a permanently false active state.
- Reconnection must resubscribe every desired channel and rebuild the confirmed set.
- Unsubscribe and disconnect cleanup must remain edge-triggered on the last local subscriber.

#### Acceptance criteria

- First subscribe fails once and then succeeds without requiring an unrelated Pub/Sub disconnect.
- A failed channel is never reported as confirmed.
- Reconnect restores all desired channels exactly once.
- Last-client unsubscribe removes both desired and confirmed state.

### REM-P1-04 — Make latency and load evidence truthful

#### Problem

The writer records latency before the WebSocket send completes. The load client sends subscriptions and sleeps without receiving or validating frames. `/metrics/summary` averages per-client percentiles rather than computing a population percentile.

#### Required behavior

- Server-side send latency is recorded only after a successful `send_text()` or `send_bytes()`.
- The benchmark client continuously receives frames for the scenario duration.
- The benchmark records received messages, malformed messages, sequence observations, disconnects, and client-side receive latency when timestamps permit.
- Benchmark exceptions must be reported, not silently swallowed.
- Metric names and result labels must distinguish `average_client_p99` from a true overall p99. Prefer computing a true percentile from collected benchmark samples.
- Existing benchmark results must be marked stale until rerun after the implementation changes.

#### Acceptance criteria

- A deliberately blocked fake WebSocket increases measured server-side latency.
- Every benchmark client proves that it received and decoded data.
- The benchmark fails or marks the scenario invalid when any client unexpectedly disconnects or receives no updates.
- `results.md` records command, environment, event-rate meaning, client receive counts, drops, and percentile method.

### REM-P1-05 — Keep the engine alive across transient Redis failures

#### Problem

`process_consumed_event()` correctly leaves an event unacknowledged when the Redis snapshot update or publish fails, but the exception currently escapes `run()` and terminates the entire engine loop. Replay remains safe, yet recovery requires an external process restart.

#### Required behavior

- Add a bounded per-event retry policy with backoff, or make supervisor-driven restart an explicit and tested lifecycle contract.
- Never acknowledge or commit the Kafka event until both Redis operations succeed.
- Preserve clean shutdown while an event is retrying; an unresolved event remains replayable.
- Expose retry/exhaustion behavior through logs or a low-cardinality metric.

#### Acceptance criteria

- A transient Redis failure recovers without losing or prematurely committing the event.
- A sustained failure leaves the offset uncommitted and does not prevent clean shutdown.
- The chosen retry or restart policy is documented in SPEC and README.

## 7. P2 Requirements

### REM-P2-01 — Align History API failure behavior

- Map database connection/unavailability errors to the documented 504 response, or revise the documented contract to a deliberate 503 response.
- Preserve 500 for unexpected programming errors.
- Validate timestamps before converting them to `datetime`; out-of-range values return 400.
- Add tests for connection failure and out-of-range timestamps.

### REM-P2-02 — Validate conditional subscription numbers

- `min_change_pct` and `max_spread` must be finite and non-negative.
- Reject invalid filters with an explicit client error instead of silently removing the filter.
- Add tests for negative values, `NaN`, positive/negative infinity, strings, and valid zero.

### REM-P2-03 — Align documentation and evidence

- Update README architecture to show the separate DistributionEngine, Redis Pub/Sub, multi-gateway fanout, TickWriter, and TimescaleDB.
- Remove already-implemented features from README future work.
- Correct the stale README test count (`40 tests` at `README.md:458` versus the verified 94-test baseline), then avoid hard-coding the count or derive it automatically.
- Define feed rate unambiguously as total events/sec or events/sec per symbol, and make code, benchmark, README, and SPEC agree.
- Update snapshot, sequence, failure, batching, and offset-commit descriptions after the P0/P1 changes.
- Remove claims that have no corresponding benchmark or test evidence.

### REM-P2-04 — Coalesce engine acknowledgements per partition

- Replace one synchronous Kafka commit per acknowledged event with one commit of the highest contiguous acknowledged offset per partition for each acknowledgement drain.
- Do not commit across a failed or still-pending event in the same partition.
- Add multi-partition and out-of-order-completion tests, then benchmark commit round trips before and after the change.

### REM-P2-05 — Clarify partial-batch shutdown replay

- The current shutdown contract intentionally leaves a partial TickWriter tail batch uncommitted for Kafka replay.
- Remove the misleading final `_flush()` call after `_stop` is set, or retain it only with a direct comment explaining why it cannot persist and commit the tail batch.
- Keep a test proving that partial-tail offsets remain uncommitted during shutdown.

### REM-P2-06 — Remove or define `dropped_ticks`

- Remove the currently unused `dropped_ticks` counter, or document it as reserved exclusively for an intentional terminal-discard policy.
- Do not expose a zero-valued counter that implies drop accounting exists when no reachable path updates it.

## 8. Implementation Phases

### Phase A — Storage durability

Implement REM-P0-01, REM-P0-02, and REM-P1-02 together because they share TickWriter batching and commit semantics.

Exit criteria:

- No TickWriter path commits unresolved valid events.
- Timeout retry, poison-message ordering, batch size, and batch deadline tests pass.

### Phase B — Live engine acknowledgement

Implement REM-P0-03.

Exit criteria:

- Kafka polling, async Redis processing, acknowledgement, commit, bounded handoff, and shutdown are independently tested.
- A Redis failure leaves the Kafka event replayable.

### Phase C — Sequence correctness

Implement REM-P0-04 and REM-P0-05 together.

Exit criteria:

- Sequence semantics are per-symbol throughout producer, snapshot, gateway, tests, samples, and history.
- Stale, duplicate, next, and gap paths are deterministic and covered.

### Phase D — Subscription recovery

Implement REM-P1-01 and REM-P1-03 together because initialization correctness depends on confirmed channel state.

Exit criteria:

- Snapshot is always the first state message.
- Events at every subscribe boundary are either covered by the snapshot or delivered afterward.
- Pub/Sub failures retry or roll back visibly.

### Phase E — Evidence and cleanup

Implement REM-P1-04 and all P2 items.

Exit criteria:

- Benchmarks read and validate frames and use accurate percentile terminology.
- README and SPEC match the implemented behavior.
- Remaining deferred gaps are explicitly labeled as future production work.

## 9. Validation Plan

### Unit tests

- TickWriter timeout, shutdown, poison-message order, size flush, and time flush.
- Engine update/publish acknowledgement and failure-without-commit.
- Per-symbol sequence generation and classification.
- Snapshot recovery followed by stale backlog.
- Subscribe initialization and Pub/Sub state transitions.
- Writer latency recorded after successful send.
- History failure mapping and filter validation.

### Integration tests

Keep Docker-dependent tests opt-in and separate from the fast unit suite.

Minimum recommended scenarios:

1. Kafka event is written to Redis, published, and delivered to a WebSocket client.
2. Engine stops after polling but before Redis acknowledgement; restart replays the event.
3. TickWriter database failure does not advance offsets; recovery persists the batch once.
4. Two gateway instances receive the same subscribed symbol through Redis Pub/Sub.
5. One gateway reconnects to Pub/Sub and restores its desired channels without affecting the other gateway.

### Benchmark validation

- Rerun serialization benchmark only if payload schema changes.
- Rerun load benchmark after sequence, subscribe, writer-latency, or fanout changes.
- A benchmark run is valid only when all clients receive messages and unexpected errors equal zero.
- Preserve raw result data or machine-readable summary alongside the Markdown report.

### Required commands

```bash
python -m compileall -q src tests
pytest -q
python -m src.benchmark.serialization_bench
python -m src.benchmark.load_bench
```

Docker integration commands should be documented with the tests added during implementation.

## 10. Definition of Done

The remediation is complete when:

- All P0 and P1 acceptance criteria are implemented and tested.
- No Kafka consumer commits an event before its required downstream operation succeeds.
- No stale or duplicate incremental can follow a newer snapshot for the same symbol.
- The subscribe protocol has a deterministic snapshot-first handoff.
- TickWriter honors both batch size and time limits without intentional data loss.
- Pub/Sub channel state distinguishes desired from confirmed subscription state.
- The load benchmark actively receives data and reports truthful delivery and latency evidence.
- The full unit suite passes.
- The opt-in integration suite passes in a running Docker environment.
- README, SPEC, and benchmark results match the final implementation.

P2 items may be completed after P0/P1 code is stable, but documentation claims must not overstate unfinished behavior.
