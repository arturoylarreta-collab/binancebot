"""FASE 6 — OrderManager: idempotency, retry, timeout, cancel/replace.

Uses deterministic sim adapters plus two tiny fakes: a flaky adapter that
rejects the first N submissions and a hanging adapter that never acks.
"""

from __future__ import annotations

import asyncio
import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType
from crypto_scalper.core.exceptions import (
    InvalidOrderError,
    OrderRejectedError,
    ExchangeConnectionError,
    OrderTimeoutError,
)
from crypto_scalper.core.models import OrderRequest
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter

SYMBOL = "BTCUSDT"


def _adapter() -> SimulatedExecutionAdapter:
    adapter = SimulatedExecutionAdapter(ExecutionConfig(slippage_pct=0.0))
    adapter.set_price(SYMBOL, 50000.0)
    return adapter


def _req(qty: float = 1.0, *, cid: str = "o-1", otype: str = OrderType.MARKET.name,
         price=None, stop=None) -> OrderRequest:
    return OrderRequest(symbol=SYMBOL, side="BUY", order_type=otype, quantity=qty,
                        price=price, stop_price=stop, client_order_id=cid, requested_ts_ms=1)


class _FlakyAdapter(SimulatedExecutionAdapter):
    def __init__(self, failures: int, **kw) -> None:
        super().__init__(**kw)
        self._failures = failures
        self._submits = 0

    async def submit(self, request: OrderRequest):
        if self._submits < self._failures:
            self._submits += 1
            raise ExchangeConnectionError("flaky venue")
        self._submits += 1
        return await super().submit(request)


class _HangingAdapter(SimulatedExecutionAdapter):
    async def submit(self, request: OrderRequest):
        await asyncio.Event().wait()  # never returns


class TestSubmitBasics:
    async def test_market_submit_returns_filled(self):
        om = OrderManager(_adapter())
        report = await om.submit(_req(cid="a"))
        assert report.status == OrderStatus.FILLED.name
        assert report.avg_price == pytest.approx(50000.0)
        assert om.get_order("a") is not None
        assert len(om.orders()) == 1

    async def test_limit_submit_rests(self):
        om = OrderManager(_adapter())
        report = await om.submit(_req(cid="b", otype=OrderType.LIMIT.name, price=49000.0))
        assert report.status == OrderStatus.NEW.name


class TestValidation:
    async def test_missing_client_order_id(self):
        om = OrderManager(_adapter())
        with pytest.raises(InvalidOrderError):
            await om.submit(_req(cid=None))

    async def test_zero_quantity(self):
        om = OrderManager(_adapter())
        with pytest.raises(InvalidOrderError):
            await om.submit(_req(qty=0.0, cid="c"))

    async def test_stop_order_requires_stop_price(self):
        om = OrderManager(_adapter())
        with pytest.raises(InvalidOrderError):
            await om.submit(_req(cid="d", otype=OrderType.STOP_MARKET.name))

    async def test_limit_requires_price(self):
        om = OrderManager(_adapter())
        with pytest.raises(InvalidOrderError):
            await om.submit(_req(cid="e", otype=OrderType.LIMIT.name))


class TestIdempotency:
    async def test_repeat_same_client_id_replays_not_resubmits(self):
        adapter = _adapter()
        om = OrderManager(adapter)
        first = await om.submit(_req(cid="i1"))
        second = await om.submit(_req(cid="i1"))
        assert first == second
        assert first.order_id == second.order_id
        assert len(adapter._orders) == 1  # only submitted once

    async def test_concurrent_duplicates_coalesce(self):
        adapter = _adapter()
        om = OrderManager(adapter)
        r1, r2 = await asyncio.gather(
            om.submit(_req(cid="i2")),
            om.submit(_req(cid="i2")),
        )
        assert r1 == r2
        assert len(adapter._orders) == 1

    async def test_failed_submit_not_remembered(self):
        adapter = _FlakyAdapter(failures=10)
        om = OrderManager(adapter, ExecutionConfig(retries=1, backoff_base_s=0.01))
        with pytest.raises(ExchangeConnectionError):
            await om.submit(_req(cid="fail"))
        assert om.get_order("fail") is None  # can be retried later


def _flaky_adapter(failures: int) -> "_FlakyAdapter":
    adapter = _FlakyAdapter(failures, config=ExecutionConfig(slippage_pct=0.0))
    adapter.set_price(SYMBOL, 50000.0)
    return adapter


class TestRetry:
    async def test_retries_then_succeeds(self):
        adapter = _flaky_adapter(failures=2)
        om = OrderManager(adapter, ExecutionConfig(retries=3, backoff_base_s=0.005))
        report = await om.submit(_req(cid="r1"))
        assert report.status == OrderStatus.FILLED.name
        assert adapter._submits == 3  # 2 failures + 1 success

    async def test_retries_exhausted_raise(self):
        adapter = _flaky_adapter(failures=5)
        om = OrderManager(adapter, ExecutionConfig(retries=1, backoff_base_s=0.005))
        with pytest.raises(ExchangeConnectionError):
            await om.submit(_req(cid="r2"))


class TestTimeout:
    async def test_submit_timeout_raises(self):
        adapter = _HangingAdapter()
        om = OrderManager(adapter, ExecutionConfig(submit_timeout_s=0.05,
                                                   retries=0, backoff_base_s=0.01))
        with pytest.raises(OrderTimeoutError):
            await om.submit(_req(cid="t1"), timeout_s=0.05)

    async def test_fill_wait_timeout_cancels(self):
        adapter = _adapter()
        om = OrderManager(adapter, ExecutionConfig(fill_timeout_s=0.05))
        adapter.schedule_partial("t2", [0.5])  # never completes -> timeout
        with pytest.raises(OrderTimeoutError):
            await om.submit(_req(cid="t2"), wait_fill=True)
        last = await adapter.get_order(SYMBOL, "t2")
        assert last.status == OrderStatus.PARTIALLY_FILLED_CANCELED.name

    async def test_wait_fill_returns_when_filled(self):
        adapter = _adapter()
        om = OrderManager(adapter, ExecutionConfig(fill_timeout_s=0.5))
        report = await om.submit(_req(cid="t3", qty=2.0), wait_fill=True)
        assert report.status == OrderStatus.FILLED.name
        assert report.executed_quantity == pytest.approx(2.0)


class TestCancel:
    async def test_cancel_resting_limit(self):
        adapter = _adapter()
        om = OrderManager(adapter)
        await om.submit(_req(cid="c1", otype=OrderType.LIMIT.name, price=40000.0))
        report = await om.cancel(SYMBOL, "c1")
        assert report.status == OrderStatus.CANCELED.name

    async def test_cancel_finalized_is_noop(self):
        adapter = _adapter()
        om = OrderManager(adapter)
        await om.submit(_req(cid="c2"))
        report = await om.cancel(SYMBOL, "c2")
        assert report.status == OrderStatus.FILLED.name

    async def test_cancel_and_replace(self):
        adapter = _adapter()
        om = OrderManager(adapter)
        old = await om.submit(_req(cid="old", otype=OrderType.LIMIT.name, price=40000.0))
        assert old.status == OrderStatus.NEW.name
        new = await om.cancel_and_replace(
            SYMBOL, "old",
            _req(cid="new", otype=OrderType.LIMIT.name, price=41000.0),
        )
        assert (await om.cancel(SYMBOL, "old")).status == OrderStatus.CANCELED.name
        assert new.status == OrderStatus.NEW.name
        assert om.get_order("new") is not None

class TestRetrySafety:
    async def test_rejection_is_never_retried(self):
        class _Rejecting(SimulatedExecutionAdapter):
            calls = 0

            async def submit(self, request):
                type(self).calls += 1
                raise OrderRejectedError("insufficient margin")

        om = OrderManager(_Rejecting(), ExecutionConfig(retries=3, backoff_base_s=0.0))
        with pytest.raises(OrderRejectedError):
            await om.submit(OrderRequest(symbol="BTCUSDT", side="BUY", order_type="MARKET",
                                         quantity=1.0, client_order_id="rej-1"))
        assert _Rejecting.calls == 1

    async def test_transient_error_after_venue_accept_does_not_double_submit(self):
        """Binance accepts a repeated client id once the first order is FILLED:
        the manager must look the order up before re-submitting."""

        class _AckLost(SimulatedExecutionAdapter):
            submits = 0

            async def submit(self, request):
                type(self).submits += 1
                await super().submit(request)
                raise ExchangeConnectionError("ack lost after accept")

        venue = _AckLost()
        venue.set_price("BTCUSDT", 100.0)
        om = OrderManager(venue, ExecutionConfig(retries=3, backoff_base_s=0.0))
        report = await om.submit(OrderRequest(symbol="BTCUSDT", side="BUY", order_type="MARKET",
                                              quantity=1.0, client_order_id="ack-1"))
        assert _AckLost.submits == 1
        assert report.status == OrderStatus.FILLED.name
