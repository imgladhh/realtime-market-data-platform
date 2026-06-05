---
name: codex-handoff
description: Generate a self-contained implementation brief for Codex. Use when a spec is ready and needs to be handed off to the Codex coding agent for implementation. Also use when asked to "prepare the handoff", "write the Codex prompt", or "format this for Codex". Produces a structured prompt that Codex can execute without access to this conversation.
---

You are producing a **Codex implementation brief** — a self-contained prompt that the Codex coding agent will receive cold, with no memory of this conversation. Everything Codex needs to understand the task, make the right tradeoffs, and produce reviewable code must be in this document.

## What to include

Read the spec (either from the conversation or from a spec file in the project). Then generate the brief using the structure below.

---

### CODEX IMPLEMENTATION BRIEF: [Feature Name]

#### Context

Brief description of the codebase (2–3 sentences). Reference:
- Entry point: `src/gateway/gateway.py` (FastAPI app, fanout loop, WebSocket handler)
- Distribution pipeline: `src/engine/engine.py` (KafkaConsumerBridge → asyncio queue)
- Per-client state: `src/gateway/session.py` (ClientSession, BoundedQueue, LatencyTracker)
- Aggregation: `src/gateway/aggregator.py` (AggregationBuffer, RAW / AGG_100MS modes)
- Snapshots: `src/engine/snapshot_store.py` (Redis HSET, 24h TTL)
- Models: `src/models.py` (MarketEvent, SnapshotData, EventType)
- Tests: `tests/` (pytest, pytest-asyncio, mocked Redis and WebSocket via `tests/conftest.py`)

#### Key invariants (do not break these)

List the invariants from SPEC.md §5 that are relevant to this feature. Always include:

1. **No blocking in the event loop.** `KafkaConsumerBridge._consume_loop` runs in a thread. Crossing to asyncio uses `asyncio.run_coroutine_threadsafe`. Do not call blocking APIs from coroutines.
2. **Per-client isolation.** Fanout calls `session.enqueue()` (non-blocking, O(1)) per client. Never `await` per-client I/O from the fanout loop.
3. **Queue writes via enqueue().** All outbound messages go through `ClientSession.enqueue()` → `_queue` → `_writer_loop` → `websocket.send_text()`. No direct sends from other paths.
4. **Shared state under lock.** `subscriptions` and `all_sessions` are always accessed under `subscriptions_lock`.
5. **Cleanup on disconnect.** On disconnect: cancel dispatch task → `aggregator.stop()` → `_cleanup(session)`. All three steps required.

Add any feature-specific invariants from the spec.

#### Task

Numbered list of concrete implementation steps. Each step must name the exact file and what to change. No vague steps.

Example:
```
1. In `src/gateway/session.py`: add `encoding: str = "json"` field to `ClientSession.__init__`. 
   Add helper `_encode(self, msg: dict) -> bytes | str` that dispatches to json or msgpack.
2. In `src/gateway/session.py`: replace `json.dumps(message)` in `_writer_loop` with 
   `self._encode(message)`. Use `ws.send_bytes()` for msgpack, `ws.send_text()` for json.
3. In `src/gateway/gateway.py`: parse `?encoding=msgpack` query param in `websocket_stream`,
   pass to `ClientSession(encoding=...)`.
4. Add `msgpack>=1.0.8` to `requirements.txt` (already installed, just not declared).
```

#### Do not change

Explicit list of files or behaviors that must not be modified:
- Do not change the `AggregationBuffer` interface (`push`, `events`, `start`, `stop`).
- Do not change the `MarketEvent` or `SnapshotData` dataclasses unless the spec explicitly requires it.
- Do not change `KafkaConsumerBridge` unless the spec requires it.
- Do not change `SnapshotStore` unless the spec requires it.
- [Add any feature-specific constraints]

#### Tests required

List every test case Codex must write. Format: `tests/<file>.py::test_<name>` — what it verifies.

Example:
```
tests/test_session.py::test_msgpack_encoding_reduces_payload_size
  - encode a MarketEvent dict with msgpack, verify len(result) < len(json.dumps(result))

tests/test_session.py::test_writer_sends_bytes_in_msgpack_mode
  - construct ClientSession(encoding="msgpack"), enqueue a message, run writer loop one tick,
    verify ws.send_bytes was called (not send_text)

tests/test_session.py::test_writer_sends_text_in_json_mode
  - same but encoding="json", verify ws.send_text called

tests/test_gateway.py::test_websocket_encoding_query_param
  - connect with ?encoding=msgpack, verify session.encoding == "msgpack"
```

#### Acceptance criteria

Bulleted checklist. Codex's implementation is done when every item is true:

- [ ] All new tests pass: `pytest tests/ -v`
- [ ] No existing tests broken
- [ ] [Feature-specific behavior verified]
- [ ] No new blocking calls in event loop (grep for `time.sleep` in changed files)
- [ ] No direct `websocket.send_text` outside `_writer_loop`

#### Return to reviewer

When implementation is complete, Codex should:
1. Run `pytest tests/ -v` and paste the output.
2. Run `git diff --stat` and list changed files.
3. Flag any spec ambiguity that required a judgment call, with what was decided.

The reviewer (Claude) will run `/market-review` on the diff before approving.

---

## Formatting rules

- The brief is a standalone document — paste it verbatim into the Codex prompt. Do not add meta-commentary about the brief itself.
- Keep the "Task" section ordered and concrete. Codex follows steps sequentially.
- The "Do not change" section is as important as "Task" — it prevents scope creep and accidental regressions.
- If the spec has open questions, resolve them before writing the brief. An ambiguous brief produces ambiguous code.
