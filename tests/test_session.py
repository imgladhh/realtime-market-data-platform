import asyncio
import json
import msgpack
import pytest
from src.gateway.session import (
    ClientSession,
    Encoding,
    SlowConsumerPolicy,
    SubscriptionFilter,
)
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from tests.conftest import make_event, make_websocket


def make_session(
    policy=SlowConsumerPolicy.DROP_OLDEST,
    queue_size=5,
    encoding=Encoding.JSON,
) -> ClientSession:
    ws = make_websocket()
    session = ClientSession(
        client_id="test-client",
        websocket=ws,
        policy=policy,
        encoding=encoding,
    )
    session._queue = asyncio.Queue(maxsize=queue_size)
    session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
    return session


# ── BoundedQueue: drop_oldest ─────────────────────────────────────────────────

class TestDropOldest:

    def test_enqueue_within_capacity(self):
        session = make_session(queue_size=3)
        for i in range(3):
            result = session.enqueue({"symbol": "AAPL", "seq": i})
            assert result is True
        assert session._queue.qsize() == 3
        assert session.stats.dropped == 0

    def test_drop_oldest_when_full(self):
        session = make_session(queue_size=3)
        for i in range(3):
            session.enqueue({"symbol": "AAPL", "seq": i, "bid": float(i)})

        # Queue full — seq=0 should be dropped, seq=3 enqueued
        session.enqueue({"symbol": "AAPL", "seq": 3, "bid": 3.0})

        assert session.stats.dropped == 1
        assert session._queue.qsize() == 3

        messages = []
        while not session._queue.empty():
            messages.append(session._queue.get_nowait())

        seqs = [m["seq"] for m in messages]
        assert 0 not in seqs
        assert 3 in seqs

    def test_drop_count_accumulates(self):
        session = make_session(queue_size=2)
        for i in range(6):
            session.enqueue({"symbol": "AAPL", "seq": i})
        assert session.stats.dropped == 4

    def test_different_symbols_enqueued_independently(self):
        session = make_session(queue_size=10)
        session.enqueue({"symbol": "AAPL", "seq": 1})
        session.enqueue({"symbol": "TSLA", "seq": 2})
        assert session._queue.qsize() == 2
        assert session.stats.dropped == 0


# ── Gap detection ─────────────────────────────────────────────────────────────

class TestGapDetection:

    def test_no_gap_on_first_message(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        assert session.check_gap("AAPL", 101) is False

    def test_no_gap_within_tolerance(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        assert session.check_gap("AAPL", 105) is False

    def test_gap_detected_beyond_tolerance(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        assert session.check_gap("AAPL", 106) is True
        assert session.stats.gaps_detected == 1

    def test_gap_updates_last_seq(self):
        session = make_session()
        session.last_seq["AAPL"] = 100
        session.check_gap("AAPL", 200)
        assert session.last_seq["AAPL"] == 200

    def test_no_gap_when_symbol_unseen(self):
        session = make_session()
        result = session.check_gap("AAPL", 500)
        assert result is False
        assert session.last_seq["AAPL"] == 500


# ── Writer loop ───────────────────────────────────────────────────────────────

class TestConditionalDelivery:

    def test_no_filter_delivers_all_events(self):
        session = make_session()
        assert session.should_deliver(make_event(symbol="AAPL", bid=100.0)) is True

    def test_min_change_filters_below_threshold(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(min_change_pct=0.05)
        session.last_price["AAPL"] = 100.0
        assert session.should_deliver(make_event(symbol="AAPL", bid=100.04)) is False

    def test_min_change_delivers_at_threshold(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(min_change_pct=0.05)
        session.last_price["AAPL"] = 100.0
        assert session.should_deliver(make_event(symbol="AAPL", bid=100.05)) is True

    def test_max_spread_filters_above_threshold(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(max_spread=0.50)
        assert session.should_deliver(
            make_event(symbol="AAPL", bid=100.0, ask=100.51)
        ) is False

    def test_max_spread_delivers_at_threshold(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(max_spread=0.50)
        assert session.should_deliver(
            make_event(symbol="AAPL", bid=100.0, ask=100.50)
        ) is True

    def test_filters_use_and_semantics(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(
            min_change_pct=0.05,
            max_spread=0.50,
        )
        session.last_price["AAPL"] = 100.0
        assert session.should_deliver(
            make_event(symbol="AAPL", bid=100.10, ask=100.70)
        ) is False
        assert session.should_deliver(
            make_event(symbol="AAPL", bid=100.10, ask=100.20)
        ) is True

    def test_min_change_delivers_when_no_last_price_baseline(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(min_change_pct=0.05)
        assert session.should_deliver(make_event(symbol="AAPL", bid=100.01)) is True

    def test_mark_delivered_updates_last_price(self):
        session = make_session()
        event = make_event(symbol="AAPL", bid=101.25)
        assert session.enqueue(event.to_dict()) is True
        session.mark_delivered(event)
        assert session.last_price["AAPL"] == 101.25

    def test_enqueue_failure_does_not_update_last_price(self):
        session = make_session(
            policy=SlowConsumerPolicy.DISCONNECT,
            queue_size=1,
        )
        session.enqueue({"symbol": "AAPL", "seq": 1})
        event = make_event(symbol="AAPL", bid=101.25)
        assert session.enqueue(event.to_dict()) is False
        assert "AAPL" not in session.last_price

    def test_filtered_event_does_not_update_last_price(self):
        session = make_session()
        session.filters["AAPL"] = SubscriptionFilter(min_change_pct=0.05)
        session.last_price["AAPL"] = 100.0
        event = make_event(symbol="AAPL", bid=100.04)
        assert session.should_deliver(event) is False
        assert session.last_price["AAPL"] == 100.0


class TestWriterLoop:

    @pytest.mark.asyncio
    async def test_writer_sends_messages(self):
        session = make_session(queue_size=10)
        session.start_writer()

        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})

        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.sent == 2
        assert len(session.websocket.sent_messages) == 2
        assert len(session.websocket.sent_bytes) == 0

    @pytest.mark.asyncio
    async def test_writer_sends_bytes_in_msgpack_mode(self):
        session = make_session(queue_size=10, encoding=Encoding.MSGPACK)
        session.start_writer()

        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})

        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.sent == 1
        assert len(session.websocket.sent_bytes) == 1
        assert len(session.websocket.sent_messages) == 0

        decoded = msgpack.unpackb(session.websocket.sent_bytes[0], raw=False)
        assert decoded["symbol"] == "AAPL"
        assert decoded["seq"] == 1

    @pytest.mark.asyncio
    async def test_writer_sends_text_in_json_mode(self):
        session = make_session(queue_size=10, encoding=Encoding.JSON)
        session.start_writer()

        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})

        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.sent == 1
        assert len(session.websocket.sent_messages) == 1
        assert len(session.websocket.sent_bytes) == 0
        assert json.loads(session.websocket.sent_messages[0])["seq"] == 1

    def test_msgpack_payload_smaller_than_json(self):
        payload = make_event(symbol="AAPL", seq=1).to_dict()

        msgpack_bytes = msgpack.packb(payload, use_bin_type=True)
        json_bytes = json.dumps(payload).encode()
        reduction = 1 - (len(msgpack_bytes) / len(json_bytes))

        assert len(msgpack_bytes) < len(json_bytes)
        assert reduction >= 0.20

    @pytest.mark.asyncio
    async def test_writer_sends_correct_content(self):
        """
        Verifies that the writer sends the actual message content correctly.
        Specifically: enqueue seq=1 then seq=2, assert both are sent
        and in order (no silent drops or replacements).
        """
        session = make_session(queue_size=10)
        session.start_writer()

        session.enqueue({"symbol": "AAPL", "seq": 1, "bid": 189.10})
        session.enqueue({"symbol": "AAPL", "seq": 2, "bid": 189.20})

        await asyncio.sleep(0.1)
        await session.close()

        assert len(session.websocket.sent_messages) == 2
        msg1 = json.loads(session.websocket.sent_messages[0])
        msg2 = json.loads(session.websocket.sent_messages[1])

        assert msg1["seq"] == 1
        assert msg1["bid"] == 189.10
        assert msg2["seq"] == 2
        assert msg2["bid"] == 189.20

    @pytest.mark.asyncio
    async def test_writer_records_latency(self):
        session = make_session(queue_size=10)
        session.start_writer()

        import time
        event_ts = int(time.time() * 1000) - 10
        session.enqueue({"symbol": "AAPL", "seq": 1, "event_ts": event_ts})

        await asyncio.sleep(0.1)
        await session.close()

        assert session.stats.latency.sample_count == 1
        assert session.stats.latency.p50 >= 10

    @pytest.mark.asyncio
    async def test_disconnect_stops_writer(self):
        session = make_session(queue_size=10)
        session.start_writer()
        await session.close()
        assert session._disconnected.is_set()

    @pytest.mark.asyncio
    async def test_stats_dict_contains_required_fields(self):
        session = make_session()
        session.aggregator = AggregationBuffer(mode=AggregationMode.RAW)
        stats = session.stats_dict()
        required = {"sent", "dropped", "gaps_detected",
                    "uptime_sec", "subscriptions", "queue_size",
                    "aggregation_mode", "latency_ms"}
        assert required.issubset(stats.keys())
        # coalesced should no longer be in stats
        assert "coalesced" not in stats
