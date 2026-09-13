"""FASE 6 — simulated exchange adapter behavior.

Offline, deterministic fill semantics: MARKET immediate fill with slippage,
LIMIT crossing, STOP_MARKET / TAKE_PROFIT_MARKET price-triggered fills,
reduce-only clamping, partial-fill scheduling, idempotency duplicates and
the event subscription stream.
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType, Side
from crypto_scalper.core.exceptions import DuplicateOrderError, ExecutionError
from crypto_scalper.core.models import OrderRequest
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter

SYMBOL = "BTCUSDT"


def _adapter(slippage: float = 0.0) -> SimulatedExecutionAdapter:
    adapter = SimulatedExecutionAdapter(ExecutionConfig(slippage_pct=slippage))
    adapter.set_price(SYMBOL, 50000.0)
    return adapter


def _req(
    qty: float = 1.0,
    side: str = "BUY",
    otype: str = OrderType.MARKET.name,
    *,
    cid: str = "o-1",
    price: float | None = None,
    stop: float | None = None,
    reduce_only: bool = False,
) -> OrderRequest:
    return OrderRequest(
        symbol=SYMBOL,
        side=side,
        order_type=otype,
        quantity=qty,
        price=price,
        stop_price=stop,
        reduce_only=reduce_only,
        client_order_id=cid,
        requested_ts_ms=1,
    )


class TestMarketFills:
    async def test_market_buy_fills_at_last_price(self):
        adapter = _adapter()
        report = await adapter.submit(_req(qty=2.0, cid="m1"))
        assert report.status == OrderStatus.FILLED.name
        assert report.executed_quantity == pytest.approx(2.0)
        assert report.avg_price == pytest.approx(50000.0)
        assert len(report.fills) == 1

    async def test_market_sell_fills(self):
        adapter = _adapter()
        report = await adapter.submit(_req(qty=3.0, side="SELL", cid="m2"))
        assert report.status == OrderStatus.FILLED.name
        assert report.executed_quantity == pytest.approx(3.0)

    async def test_slippage_direction(self):
        adapter = _adapter(slippage=0.001)
        buy = await adapter.submit(_req(qty=1.0, cid="s1"))
        sell = await adapter.submit(_req(qty=1.0, side="SELL", cid="s2"))
        assert buy.avg_price == pytest.approx(50050.0)
        assert sell.avg_price == pytest.approx(49950.0)


class TestLimitOrders:
    async def test_limit_buy_rests_then_fills_on_dip(self):
        adapter = _adapter()
        report = await adapter.submit(_req(otype=OrderType.LIMIT.name,
                                           price=49000.0, cid="l1"))
        assert report.status == OrderStatus.NEW.name
        adapter.set_price(SYMBOL, 49500.0)
        assert (await adapter.get_order(SYMBOL, "l1")).status == OrderStatus.NEW.name
        adapter.set_price(SYMBOL, 48900.0)
        filled = await adapter.get_order(SYMBOL, "l1")
        assert filled.status == OrderStatus.FILLED.name
        assert filled.avg_price == pytest.approx(48900.0)

    async def test_limit_sell_fills_on_rise(self):
        adapter = _adapter()
        await adapter.submit(_req(otype=OrderType.LIMIT.name,
                                  price=51000.0, cid="l2"))
        adapter.set_price(SYMBOL, 51200.0)
        filled = await adapter.get_order(SYMBOL, "l2")
        assert filled.status == OrderStatus.FILLED.name

    async def test_cancel_resting_limit(self):
        adapter = _adapter()
        await adapter.submit(_req(otype=OrderType.LIMIT.name, price=48000.0, cid="l3"))
        cancelled = await adapter.cancel(SYMBOL, "l3")
        assert cancelled.status == OrderStatus.CANCELED.name
        assert await adapter.open_orders(SYMBOL) == ()


class TestStopAndTakeProfit:
    async def test_long_stop_loss_trigger(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=5.0, cid="entry"))
        sl = await adapter.submit(_req(
            qty=5.0, side="SELL", otype=OrderType.STOP_MARKET.name,
            stop=49500.0, reduce_only=True, cid="sl"))
        assert sl.status == OrderStatus.NEW.name
        adapter.set_price(SYMBOL, 49400.0)
        filled = await adapter.get_order(SYMBOL, "sl")
        assert filled.status == OrderStatus.FILLED.name
        assert filled.avg_price == pytest.approx(49400.0)
        positions = await adapter.open_positions()
        assert positions == ()  # long fully closed

    async def test_short_stop_loss_trigger(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=5.0, side="SELL", cid="entry"))
        sl = await adapter.submit(_req(
            qty=5.0, side="BUY", otype=OrderType.STOP_MARKET.name,
            stop=50500.0, reduce_only=True, cid="sl"))
        assert sl.status == OrderStatus.NEW.name
        adapter.set_price(SYMBOL, 50600.0)
        filled = await adapter.get_order(SYMBOL, "sl")
        assert filled.status == OrderStatus.FILLED.name
        assert await adapter.open_positions() == ()

    async def test_take_profit_trigger(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=5.0, cid="entry"))
        tp = await adapter.submit(_req(
            qty=5.0, side="SELL", otype=OrderType.TAKE_PROFIT_MARKET.name,
            stop=51000.0, reduce_only=True, cid="tp"))
        assert tp.status == OrderStatus.NEW.name
        adapter.set_price(SYMBOL, 51200.0)
        filled = await adapter.get_order(SYMBOL, "tp")
        assert filled.status == OrderStatus.FILLED.name

    async def test_reduce_only_never_opens_new_side(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=5.0, cid="entry"))
        await adapter.submit(_req(qty=5.0, side="SELL",
                                  otype=OrderType.STOP_MARKET.name,
                                  stop=49500.0, reduce_only=True, cid="sl-a"))
        await adapter.submit(_req(qty=5.0, side="SELL",
                                  otype=OrderType.STOP_MARKET.name,
                                  stop=49500.0, reduce_only=True, cid="sl-b"))
        adapter.set_price(SYMBOL, 49400.0)  # both trigger; only one can reduce
        a = await adapter.get_order(SYMBOL, "sl-a")
        b = await adapter.get_order(SYMBOL, "sl-b")
        assert a.status == OrderStatus.FILLED.name
        assert b.executed_quantity == 0.0


class TestPartialFills:
    async def test_scheduled_partial_then_complete(self):
        adapter = _adapter()
        adapter.schedule_partial("p1", [0.4, 0.6])
        report = await adapter.submit(_req(qty=1.0, cid="p1"))
        assert report.status == OrderStatus.PARTIALLY_FILLED.name
        assert report.executed_quantity == pytest.approx(0.4)
        final = adapter.complete_partial("p1")
        assert final.status == OrderStatus.FILLED.name
        assert final.executed_quantity == pytest.approx(1.0)

    async def test_partial_fill_emits_multiple_legs(self):
        adapter = _adapter()
        adapter.schedule_partial("p2", [0.5, 1.5])
        report = await adapter.submit(_req(qty=2.0, cid="p2"))
        adapter.complete_partial("p2")
        filled = await adapter.get_order(SYMBOL, "p2")
        assert len(filled.fills) == 2
        assert filled.executed_quantity == pytest.approx(2.0)


class TestDuplicatesAndErrors:
    async def test_duplicate_client_order_id_rejected(self):
        adapter = _adapter()
        await adapter.submit(_req(cid="dup"))
        with pytest.raises(DuplicateOrderError):
            await adapter.submit(_req(cid="dup"))

    async def test_missing_price_raises(self):
        adapter = SimulatedExecutionAdapter()
        with pytest.raises(ExecutionError):
            await adapter.submit(_req(cid="noprice"))

    async def test_open_positions_reflects_short_and_long(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=2.0, cid="long"))
        await adapter.submit(_req(qty=1.0, side="SELL", cid="short"))
        positions = {p["symbol"]: p for p in await adapter.open_positions()}
        assert positions[SYMBOL]["quantity"] == pytest.approx(1.0)
        assert positions[SYMBOL]["side"] == "LONG"

    async def test_open_orders_lists_resting_only(self):
        adapter = _adapter()
        await adapter.submit(_req(qty=1.0, cid="entry"))
        await adapter.submit(_req(otype=OrderType.STOP_MARKET.name, side="SELL",
                                  stop=49000.0, reduce_only=True, cid="sl"))
        resting = await adapter.open_orders()
        assert {r.client_order_id for r in resting} == {"sl"}


class TestEventStream:
    async def test_subscribe_receives_status_changes(self):
        adapter = _adapter()
        q = adapter.subscribe()
        await adapter.submit(_req(qty=1.0, cid="ev1"))
        event = await asyncio_wait_first(q)
        assert event.client_order_id == "ev1"
        assert event.status == OrderStatus.FILLED.name

    async def test_subscribe_sees_protection_trigger(self):
        adapter = _adapter()
        q = adapter.subscribe()
        await adapter.submit(_req(qty=1.0, cid="entry"))
        await adapter.submit(_req(qty=1.0, side="SELL",
                                  otype=OrderType.STOP_MARKET.name,
                                  stop=49000.0, reduce_only=True, cid="sl"))
        adapter.set_price(SYMBOL, 48000.0)
        while True:
            event = await asyncio_wait_first(q)
            if event.client_order_id == "sl" and event.status == OrderStatus.FILLED.name:
                break
        assert event.executed_quantity == pytest.approx(1.0)


async def asyncio_wait_first(q: object) -> object:
    import asyncio
    return await asyncio.wait_for(q.get(), timeout=2.0)