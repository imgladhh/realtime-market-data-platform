import asyncio
import pytest
from src.gateway.aggregator import AggregationBuffer, AggregationMode
from tests.conftest import make_event


class TestAggregationBufferRaw:

    @pytest.mark.asyncio
    async def test_raw_passes_through_immediately(self):
        buf = AggregationBuffer(mode=AggregationMode.RAW)
        buf.start()

        event = make_event(symbol="AAPL", seq=1)
        await buf.push(event)

        # In RAW mode, event should be immediately available
        result = await asyncio.wait_for(buf._output.get(), timeout=0.5)
        assert result.seq == 1
        assert result.symbol == "AAPL"
        buf.stop()

    @pytest.mark.asyncio
    async def test_raw_preserves_order(self):
        buf = AggregationBuffer(mode=AggregationMode.RAW)
        buf.start()

        for i in range(5):
            await buf.push(make_event(seq=i))

        results = []
        for _ in range(5):
            results.append(await asyncio.wait_for(buf._output.get(), timeout=0.5))

        seqs = [r.seq for r in results]
        assert seqs == list(range(5))
        buf.stop()

    @pytest.mark.asyncio
    async def test_raw_no_flush_task(self):
        buf = AggregationBuffer(mode=AggregationMode.RAW)
        buf.start()
        assert buf._flush_task is None
        buf.stop()


class TestAggregationBufferAgg100ms:

    @pytest.mark.asyncio
    async def test_agg_buffers_events(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()

        # Push 3 AAPL events rapidly
        for i in range(3):
            await buf.push(make_event(symbol="AAPL", seq=i, bid=189.0 + i))

        # Should NOT be available immediately (still buffering)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(buf._output.get(), timeout=0.05)

        buf.stop()

    @pytest.mark.asyncio
    async def test_agg_emits_latest_per_symbol(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()

        # Push 3 AAPL events — only the latest should be emitted
        for i in range(3):
            await buf.push(make_event(symbol="AAPL", seq=i, bid=189.0 + i))

        # Wait for flush
        await asyncio.sleep(0.15)

        result = await asyncio.wait_for(buf._output.get(), timeout=0.5)
        assert result.seq == 2        # latest seq
        assert result.bid == 191.0    # latest bid (189.0 + 2)

        # Only one event should have been emitted
        assert buf._output.empty()
        buf.stop()

    @pytest.mark.asyncio
    async def test_agg_handles_multiple_symbols(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()

        await buf.push(make_event(symbol="AAPL", seq=1, bid=189.0))
        await buf.push(make_event(symbol="TSLA", seq=2, bid=175.0))
        await buf.push(make_event(symbol="AAPL", seq=3, bid=190.0))  # latest AAPL

        await asyncio.sleep(0.15)

        results = []
        while not buf._output.empty():
            results.append(buf._output.get_nowait())

        symbols = {r.symbol for r in results}
        assert "AAPL" in symbols
        assert "TSLA" in symbols
        assert len(results) == 2  # one per symbol

        aapl = next(r for r in results if r.symbol == "AAPL")
        assert aapl.seq == 3  # latest AAPL
        buf.stop()

    @pytest.mark.asyncio
    async def test_agg_clears_buffer_after_flush(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()

        await buf.push(make_event(symbol="AAPL", seq=1))
        await asyncio.sleep(0.15)

        # Buffer should be cleared after flush
        assert len(buf._buffer) == 0
        buf.stop()

    @pytest.mark.asyncio
    async def test_agg_has_flush_task(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()
        assert buf._flush_task is not None
        buf.stop()

    @pytest.mark.asyncio
    async def test_agg_stop_cancels_flush_task(self):
        buf = AggregationBuffer(mode=AggregationMode.AGG_100MS, interval_ms=100)
        buf.start()
        buf.stop()
        await asyncio.sleep(0.05)
        assert buf._flush_task.cancelled() or buf._flush_task.done()
