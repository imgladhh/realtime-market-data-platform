from src.feed import simulator


def test_feed_sequences_are_monotonic_per_symbol(monkeypatch):
    monkeypatch.setattr(simulator, "make_producer", lambda _servers: object())
    feed = simulator.FeedSimulator()

    events = [
        feed._make_event("AAPL"),
        feed._make_event("TSLA"),
        feed._make_event("AAPL"),
        feed._make_event("TSLA"),
        feed._make_event("AAPL"),
        feed._make_event("TSLA"),
    ]

    assert [event.seq for event in events if event.symbol == "AAPL"] == [1, 2, 3]
    assert [event.seq for event in events if event.symbol == "TSLA"] == [1, 2, 3]
