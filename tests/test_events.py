"""Offline tests for the EventBus and WebSocket frame parsing."""

import asyncio

import pytest

from crypto_scalper.core.events import EventBus, LifecycleEvent, LIFECYCLE, QueueFullError, TradeEvent
from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.models import AggTrade
from crypto_scalper.market_data.websocket import _parse_agg_trade, _parse_depth_update


class TestEventBus:
    async def test_publish_subscribe(self):
        bus = EventBus(queue_maxsize=4)
        q = bus.subscribe("market.BTCUSDT.trade")
        ev = TradeEvent(
            topic="market.BTCUSDT.trade",
            trade=AggTrade(symbol="BTCUSDT", event_time_ms=1, trade_id=1, price=100.0, quantity=1.0),
        )
        await bus.publish(ev)
        got = q.get_nowait()
        assert got.topic == "market.BTCUSDT.trade"
        assert got.trade.price == 100.0

    async def test_unsubscribe(self):
        bus = EventBus()
        q = bus.subscribe("a")
        bus.unsubscribe("a", q)
        assert bus.subscriber_count("a") == 0

    async def test_publish_nowait_full_raises(self):
        bus = EventBus(queue_maxsize=1)
        q = bus.subscribe("x")
        bus.publish_nowait(LifecycleEvent(topic="x", kind="k"))
        with pytest.raises(QueueFullError):
            bus.publish_nowait(LifecycleEvent(topic="x", kind="k"))


class TestFrameParsing:
    def test_agg_trade_buy(self):
        payload = {
            "e": "aggTrade", "E": 1700000000000, "s": "btcusdt",
            "a": 123, "p": "65000.0", "q": "0.5", "f": 1, "l": 2,
            "T": 1700000000001, "m": False, "M": True,
        }
        event = _parse_agg_trade(payload)
        assert event is not None
        assert event.symbol == "BTCUSDT"
        assert event.price == 65000.0
        assert event.quantity == 0.5
        assert event.aggressor is AggressorSide.BUY
        assert event.trade_id == 123

    def test_agg_trade_sell(self):
        payload = {"e": "aggTrade", "s": "ethusdt", "a": 1, "p": "3000", "q": "1", "T": 1, "m": True}
        event = _parse_agg_trade(payload)
        assert event.aggressor is AggressorSide.SELL

    def test_depth_update(self):
        payload = {
            "e": "depthUpdate", "E": 1700000000005, "s": "btcusdt",
            "U": 156, "u": 160, "pu": 155,
            "b": [["100.0", "5.0"], ["99.0", "0.0"]],
            "a": [["100.5", "3.0"]],
        }
        event = _parse_depth_update(payload)
        assert event is not None
        assert event.first_update_id == 156
        assert event.final_update_id == 160
        assert event.previous_final_update_id == 155
        assert len(event.bids) == 2
        assert event.asks[0] == (100.5, 3.0)

    def test_malformed_returns_none(self):
        assert _parse_agg_trade({"e": "aggTrade"}) is None
        assert _parse_depth_update({"e": "depthUpdate"}) is None