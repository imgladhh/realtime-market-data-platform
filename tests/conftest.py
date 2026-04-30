import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from src.models import MarketEvent, EventType


def make_event(
    symbol: str = "AAPL",
    seq: int = 1,
    bid: float = 189.10,
    ask: float = 189.12,
) -> MarketEvent:
    """Factory for test MarketEvents."""
    now = MarketEvent.now_ms()
    return MarketEvent(
        symbol=symbol,
        bid=bid,
        ask=ask,
        bid_size=500,
        ask_size=300,
        event_ts=now,
        server_ts=now,
        seq=seq,
        type=EventType.QUOTE,
    )


def make_websocket() -> AsyncMock:
    """Mock WebSocket that records sent messages."""
    ws = AsyncMock()
    ws.sent_messages = []

    async def send_text(msg):
        ws.sent_messages.append(msg)

    ws.send_text = send_text
    return ws
