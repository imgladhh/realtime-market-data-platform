---
name: spec
description: Write a technical spec for a new feature in this real-time market data platform. Use when asked to design, plan, or spec out any extension — e.g. "spec out msgpack support", "write a spec for Prometheus metrics", "design the conditional subscriptions feature". Produces a structured document in the format of SPEC.md that can be handed off to Codex for implementation.
---

You are writing a technical specification for the **Real-Time Market Data Platform** (see `SPEC.md` for the full system context). Your output is the primary artifact that the Codex coding agent will use to implement the feature — it must be precise, complete, and unambiguous.

## Before writing

Read the relevant source files to understand current implementation details:
- `src/models.py` — MarketEvent, SnapshotData, EventType
- `src/engine/engine.py` — KafkaConsumerBridge, DistributionEngine
- `src/engine/snapshot_store.py` — Redis read/write patterns
- `src/gateway/gateway.py` — fanout_loop, client_dispatch_loop, REST endpoints, WebSocket handler
- `src/gateway/session.py` — ClientSession, BoundedQueue, LatencyTracker
- `src/gateway/aggregator.py` — AggregationBuffer, AggregationMode

Understand what currently exists before specifying what changes.

## Spec format

Use this structure. Include every section; write "No changes required" if a section is unaffected.

---

### 1. Problem Statement

One paragraph. What problem does this feature solve? Why does the current system fall short? Reference concrete limitations (latency numbers, missing capabilities, operational gaps).

### 2. Requirements

**Functional requirements** — numbered FR-N table. Each row: ID | Requirement. Be specific enough that a test can verify it.

**Non-functional requirements** — numbered NFR-N table. Include latency budgets, throughput targets, backward-compatibility constraints.

### 3. Architecture Changes

Show a diff to the component diagram if topology changes. Describe which components are added, modified, or unaffected. If a new component is introduced, describe its responsibility and technology choice.

### 4. Data Model Changes

For each changed struct/dataclass/Redis schema: show before → after. Explain why each field is added or removed. State any invariants (monotonicity, TTL, atomicity guarantees).

### 5. Key Design Decisions

One subsection per decision. Format:
- **Decision:** what was chosen
- **Why:** the reasoning, including rejected alternatives
- **Tradeoff:** what you give up

Flag decisions that interact with existing invariants from SPEC.md sections 5.1–5.6 (Kafka partition strategy, snapshot+incremental protocol, per-client bounded queue, asyncio/Kafka thread bridge, per-client aggregation, no coalescing in ClientSession).

### 6. Failure Handling

Table: Scenario | Detection | Recovery. Cover at minimum: new failure modes introduced by this feature, and whether existing failure modes (client disconnect, slow consumer, Kafka lag, Redis restart) are affected.

### 7. API Changes

For REST: show new/modified endpoints with request/response examples.  
For WebSocket: show new message types with JSON examples.  
If no changes, state explicitly.

### 8. Implementation Plan

Ordered list of concrete steps Codex should follow. Each step names the exact file(s) to change and what to do — no vague actions like "add support for X". Steps should be small enough to verify independently.

Example:
1. Add `encoding: Literal["json", "msgpack"] = "json"` to `ClientSession.__init__` (`src/gateway/session.py`)
2. Wrap `json.dumps(message)` in `_writer_loop` with an `encode(message, self.encoding)` helper
3. Add `mode` query param parsing in `websocket_stream` (`src/gateway/gateway.py`)
4. ...

### 9. Test Plan

List test cases Codex must implement. For each: file, test name, what it verifies. Cover happy path, edge cases, and failure modes.

### 10. Performance Impact

State expected impact on p50/p99 dispatch latency. If the feature adds work to the hot path (fanout_loop or client_dispatch_loop), quantify or bound it. The NFR-1 target is p99 < 15ms under 20 clients.

---

## Style rules

- Be concrete. "Faster serialization" is not a requirement. "Encode outbound WebSocket messages using msgpack instead of JSON, reducing payload size by ≥20%" is.
- Ref existing invariants by section number (e.g. "per SPEC.md §5.3, the queue must remain non-blocking").
- If a decision is genuinely open, say so and list the options with tradeoffs — don't pretend there's one obvious answer.
- Do not add features beyond the stated scope. If you notice a natural extension, note it in a "Future Extensions" appendix but do not spec it out.
