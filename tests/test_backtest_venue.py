"""FASE 8 — BarVenue: bar-driven historical execution semantics."""

from __future__ import annotations

import pytest

from crypto_scalper.backtest.venue import BarVenue
from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType, Side
from crypto_scalper.core.models import Candle, OrderRequest


def _candle(high, low, o=None, c=None, interval_s=60, ts=1_000_000):
    o = o if o is not None else (high + low) / 2
    c = c if c is not None else (high + low) / 2
    return Candle(
        symbol="BTCUSDT", ts_ms=ts, interval_s=interval_s,
        open=o, high=high, low=low, close=c,
        volume=10.0, quote_volume=0.0, trade_count=50, completed=True,
    )


def _req(cid, side, otype, qty=2.0, price=None, stop=None, reduce_only=False):
    return OrderRequest(
        symbol="BTCUSDT", side=side, order_type=otype, quantity=qty,
        price=price, stop_price=stop, reduce_only=reduce_only,
        client_order_id=cid,
    )


async def _entry(venue, qty=2.0, fill="long"):
    venue.set_price("BTCUSDT", 100.0)
    side = Side.BUY.name if fill == "long" else Side.SELL.name
    return await venue.submit(_req("ENTRY", side, OrderType.MARKET.name, qty=qty))


class TestMarketEntry:
    async def test_buy_fills_with_slippage(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.001))
        report = await _entry(venue)
        assert report.status == OrderStatus.FILLED.name
        assert report.executed_quantity == 2.0
        assert report.avg_price == pytest.approx(100.0 * 1.001)
        positions = await venue.open_positions()
        assert positions[0]["side"] == "LONG"
        assert positions[0]["quantity"] == 2.0

    async def test_sell_fills_slipping_down(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.001))
        report = await _entry(venue, fill="short")
        assert report.avg_price == pytest.approx(100.0 * 0.999)

    async def test_fill_uses_historical_clock(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0), clock_ms=1234)
        report = await _entry(venue)
        assert report.fills[0].ts_ms == 1234


class TestStopFirstSemantics:
    async def test_stop_loss_fills_on_bar_low(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        await _entry(venue)
        await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        changed = venue.process_bar("BTCUSDT", _candle(high=101, low=97, o=100, c=100))
        cids = {r.client_order_id for r in changed}
        assert "SL" in cids
        sl = await venue.get_order("BTCUSDT", "SL")
        assert sl.status == OrderStatus.FILLED.name
        assert sl.avg_price == pytest.approx(98.0)
        assert await venue.open_positions() == ()

    async def test_take_profit_fills_on_bar_high(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        await _entry(venue)
        await venue.submit(_req("TP", Side.SELL.name, OrderType.TAKE_PROFIT_MARKET.name,
                                stop=102.0, reduce_only=True))
        venue.process_bar("BTCUSDT", _candle(high=103, low=99, o=100, c=100))
        tp = await venue.get_order("BTCUSDT", "TP")
        assert tp.status == OrderStatus.FILLED.name
        assert tp.avg_price == pytest.approx(102.0)

    async def test_sl_and_tp_in_same_bar_resolves_sl_first(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        await _entry(venue)
        await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        await venue.submit(_req("TP", Side.SELL.name, OrderType.TAKE_PROFIT_MARKET.name,
                                stop=103.0, reduce_only=True))
        changed = venue.process_bar("BTCUSDT", _candle(high=104, low=97, o=100, c=100))
        # SL resolved first → net to zero; TP cannot pivot (reduce_only clamp).
        sl = await venue.get_order("BTCUSDT", "SL")
        assert sl.status == OrderStatus.FILLED.name
        tp = await venue.get_order("BTCUSDT", "TP")
        assert tp.status != OrderStatus.FILLED.name
        assert await venue.open_positions() == ()

    async def test_bar_without_crossing_leaves_orders_resting(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        await _entry(venue)
        await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        venue.process_bar("BTCUSDT", _candle(high=101, low=99, o=100, c=100))
        sl = await venue.get_order("BTCUSDT", "SL")
        assert sl.status == OrderStatus.NEW.name
        assert len(await venue.open_positions()) == 1

    async def test_market_stop_trigger_gets_slippage(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.001))
        await _entry(venue)
        await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        venue.process_bar("BTCUSDT", _candle(high=101, low=97, o=100, c=100))
        sl = await venue.get_order("BTCUSDT", "SL")
        assert sl.avg_price == pytest.approx(98.0 * 0.999)


class TestLimitOrders:
    async def test_resting_limit_fills_at_limit_without_slippage(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.01))
        venue.set_price("BTCUSDT", 100.0)
        await venue.submit(_req("LIM", Side.BUY.name, OrderType.LIMIT.name,
                                qty=1.0, price=98.0))
        changed = venue.process_bar("BTCUSDT", _candle(high=100, low=97, o=99, c=99))
        assert "LIM" in {r.client_order_id for r in changed}
        lim = await venue.get_order("BTCUSDT", "LIM")
        assert lim.status == OrderStatus.FILLED.name
        assert lim.avg_price == pytest.approx(98.0)  # el limit no se desliza

    async def test_uncrossed_limit_stays_resting(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        venue.set_price("BTCUSDT", 100.0)
        await venue.submit(_req("LIM", Side.BUY.name, OrderType.LIMIT.name,
                                qty=1.0, price=95.0))
        venue.process_bar("BTCUSDT", _candle(high=102, low=99, o=100, c=101))
        lim = await venue.get_order("BTCUSDT", "LIM")
        assert lim.status == OrderStatus.NEW.name

    async def test_next_open_set_price_crosses_limit(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        venue.set_price("BTCUSDT", 100.0)
        await venue.submit(_req("LIM", Side.BUY.name, OrderType.LIMIT.name,
                                qty=1.0, price=98.0))
        venue.set_price("BTCUSDT", 97.0)
        lim = await venue.get_order("BTCUSDT", "LIM")
        assert lim.status == OrderStatus.FILLED.name


class TestPartialEntries:
    async def test_partial_entry_then_cancel_remaining(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0), entry_fill_fraction=0.5)
        report = await _entry(venue, qty=2.0)
        assert report.executed_quantity == 1.0
        assert report.status == OrderStatus.PARTIALLY_FILLED.name
        canceled = await venue.cancel("BTCUSDT", "ENTRY")
        assert canceled.status == OrderStatus.PARTIALLY_FILLED_CANCELED.name
        assert await venue.open_positions()  # reduce-only protection intact

    async def test_full_fraction_is_filled(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0), entry_fill_fraction=1.0)
        report = await _entry(venue, qty=2.0)
        assert report.status == OrderStatus.FILLED.name
        assert report.executed_quantity == 2.0


class TestInterface:
    async def test_open_orders_filters_by_symbol(self):
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        venue.set_price("BTCUSDT", 100.0)
        await venue.submit(_req("SL_A", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        await venue.submit(_req("SL_B", Side.SELL.name,
                                OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        assert all(r.symbol == "BTCUSDT" for r in await venue.open_orders())
        assert len(await venue.open_orders()) == 2

    async def test_duplicate_submit_rejected(self):
        from crypto_scalper.core.exceptions import DuplicateOrderError
        venue = BarVenue(ExecutionConfig(slippage_pct=0.0))
        await _entry(venue)
        await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                stop=98.0, reduce_only=True))
        with pytest.raises(DuplicateOrderError):
            await venue.submit(_req("SL", Side.SELL.name, OrderType.STOP_MARKET.name,
                                    stop=98.0, reduce_only=True))