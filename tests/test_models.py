import pytest
from src.models import MarketEvent, SnapshotData, EventType
from tests.conftest import make_event


class TestMarketEvent:

    def test_to_dict_contains_required_fields(self):
        event = make_event(symbol="AAPL", seq=1)
        d = event.to_dict()

        required = {"symbol", "bid", "ask", "bid_size", "ask_size",
                    "event_ts", "server_ts", "seq", "type"}
        assert required.issubset(d.keys())

    def test_to_dict_type_is_string(self):
        event = make_event()
        d = event.to_dict()
        assert isinstance(d["type"], str)
        assert d["type"] == "quote"

    def test_event_type_values(self):
        assert EventType.QUOTE == "quote"
        assert EventType.TRADE == "trade"
        assert EventType.SNAPSHOT == "snapshot"

    def test_now_ms_returns_int(self):
        ts = MarketEvent.now_ms()
        assert isinstance(ts, int)
        assert ts > 0

    def test_seq_preserved(self):
        event = make_event(seq=99999)
        assert event.seq == 99999
        assert event.to_dict()["seq"] == 99999

    def test_symbol_preserved(self):
        event = make_event(symbol="BTCUSD")
        assert event.symbol == "BTCUSD"
        assert event.to_dict()["symbol"] == "BTCUSD"
