import asyncio
import json
import pytest
import pytest_asyncio
from src.gateway.session import ClientSession, SlowConsumerPolicy
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from tests.conftest import make_event, make_websocket


def make_session(
    policy=SlowConsumerPolicy.DROP_OLDEST,
    coalescing=True,
    queue_size=5,  # small for testing
) -> ClientSession:
    ws = make_websocket()
    session = ClientSession(
        client_id="test-client",
        websocket=ws,
        policy=policy,
        coalescing=coalescing,
    )
    # Override queue size for testing
    session._queue = asyncio.Queue(maxsize=queue_size)
    session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
    return session


# ── BoundedQueue: drop_oldest ─────────────────────────────────────────────────

class TestDropOldest:

    def test_enqueue_within_capacity(self):
        session = make_session(queue_size=3, coalescing=False)
        for i in range(3):
            result = session.enqueue({"symbol": "AAPL", "seq": i})
            assert result is True
        assert session._queue.qsize() == 3
        assert session.stats.dropped == 0

    def test_drop_oldest_when_full(self):
        session = make_session(queue_size=3, coalescing=False)
        # Fill queue with seq 0,1,2
        for i in range(3):
            session.enqueue({"symbol": "AAPL", "seq": i, "bid": float(i)})

        # Enqueue one more — should drop seq=0 and enqueue seq=3
        session.enqueue({"symbol": "AAPL", "seq": 3, "bid": 3.0})

        assert session.stats.dropped == 1
        assert session._queue.qsize() == 3

        # Oldest (seq=0) should be gone, newest (seq=3) should be present
        messages = []
        while not session._queue.empty():
            messages.append(session._queue.get_nowait())

        seqs = [m["seq"] for m in messages]
        assert 0 not in seqs
        assert 3 in seqs

    def test_drop_count_accumulates(self):
        session = make_session(queue_size=2, coalescing=False)
        for i in range(6):
            session.enqueue({"symbol": "AAPL", "seq": i})
        assert session.stats.dropped == 4


# ── Coalescing ────────────────────────────────────────────────────────────────

class TestCoalescing:

    def test_coalescing_replaces_pending(self):
        session = make_session(queue_size=10, coalescing=True)

        # Enqueue first AAPL message — goes into queue and _pending
        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        assert session._queue.qsize() == 1
        assert session.stats.coalesced == 0

        # Enqueue second AAPL message — should coalesce (replace in _pending)
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})
        assert session.stats.coalesced == 1
        # Queue still has 1 item (the first one), _pending has the latest
        assert session._queue.qsize() == 1

    def test_different_symbols_not_coalesced(self):
        session = make_session(queue_size=10, coalescing=True)
        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        session.enqueue({"symbol": "TSLA", "seq": 2, "bid": 175.00})
        assert session.stats.coalesced == 0
        assert session._queue.qsize() == 2

    def test_coalescing_disabled(self):
        session = make_session(queue_size=10, coalescing=False)
        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})
        assert session.stats.coalesced == 0
        assert session._queue.qsize() == 2

    def test_pending_cleared_after_dequeue(self):
        """After writer drains the queue, _pending should be cleared."""
        session = make_session(queue_size=10, coalescing=True)
        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})

        # Simulate writer dequeuing
        msg = session._queue.get_nowait()
        symbol = msg.get("symbol")
        session._pending.pop(symbol, None)

        # Now a new AAPL message should NOT coalesce
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})
        assert session.stats.coalesced == 0
        assert session._queue.qsize() == 1


# ── Gap detection ─────────────────────────────────────────────────────────────

class TestGapDetection:

    def test_no_gap_on_first_message(self):
        session = make_session()
        # Seed last_seq as subscribe would
        session.last_seq["AAPL"] = 100
        assert session.check_gap("AAPL", 101) is False

    def test_no_gap_within_tolerance(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        # Tolerance is 5, so seq=105 should not trigger gap
        assert session.check_gap("AAPL", 105) is False

    def test_gap_detected_beyond_tolerance(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        # seq=106 is 6 ahead, beyond tolerance of 5
        assert session.check_gap("AAPL", 106) is True
        assert session.stats.gaps_detected == 1

    def test_gap_updates_last_seq(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        session.check_gap("AAPL", 200)
        assert session.last_seq["AAPL"] == 200

    def test_no_gap_when_symbol_unseen(self):
        session = make_session()
        # No last_seq entry — first message, no gap possible
        result = session.check_gap("AAPL", 500)
        assert result is False
        assert session.last_seq["AAPL"] == 500


# ── Writer loop ───────────────────────────────────────────────────────────────

class TestWriterLoop:

    @pytest.mark.asyncio
    async def test_writer_sends_messages(self):
        session = make_session(queue_size=10, coalescing=False)
        session.start_writer()

        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})

        # Give writer time to drain
        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.sent == 2
        assert len(session.websocket.sent_messages) == 2

    @pytest.mark.asyncio
    async def test_writer_records_latency(self):
        session = make_session(queue_size=10, coalescing=False)
        session.start_writer()

        import time
        event_ts = int(time.time() * 1000) - 10  # 10ms ago
        session.enqueue({"symbol": "AAPL", "seq": 1, "event_ts": event_ts})

        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.latency.sample_count == 1
        assert session.stats.latency.p50 >= 10  # at least 10ms

    @pytest.mark.asyncio
    async def test_disconnect_stops_writer(self):
        session = make_session(queue_size=10, coalescing=False)
        session.start_writer()
        await session.close()
        assert session._disconnected.is_set()

    @pytest.mark.asyncio
    async def test_stats_dict_contains_required_fields(self):
        session = make_session()
        session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
        stats = session.stats_dict()
        required = {"sent", "dropped", "coalesced", "gaps_detected",
                    "uptime_sec", "subscriptions", "queue_size",
                    "aggregation_mode", "latency_ms"}
        assert required.issubset(stats.keys())
