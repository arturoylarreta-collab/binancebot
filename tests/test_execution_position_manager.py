"""FASE 6 — PositionManager: the mandatory trade lifecycle.

Covers the full Sequence, forward-only transitions, the never-without-SL/TP
invariant, stop/tp-triggered closes, manual close, partial-entry handling
and the PortfolioState rebuild used by RiskEngine.
"""

from __future__ import annotations

import asyncio
import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import (
    OrderType,
    PositionStatus,
    RiskVerdict,
    Side,
    SignalType,
)
from crypto_scalper.core.exceptions import (
    InvalidOrderError,
    OrderRejectedError,
    PositionNotProtectedError,
)
from crypto_scalper.core.models import OrderRequest, RiskDecision, Signal
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter

SYMBOL = "BTCUSDT"
ENTRY = 50000.0


def _cfg(**kw) -> ExecutionConfig:
    defaults = dict(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
    defaults.update(kw)
    return ExecutionConfig(**defaults)


def _stack(config: ExecutionConfig | None = None):
    config = config or _cfg()
    adapter = SimulatedExecutionAdapter(config)
    adapter.set_price(SYMBOL, ENTRY)
    om = OrderManager(adapter, config)
    pm = PositionManager(om, config)
    return adapter, om, pm


def _signal(stype: SignalType = SignalType.LONG, symbol: str = SYMBOL) -> Signal:
    return Signal(symbol=symbol, timestamp_ms=1, signal_type=stype, score=70.0,
                  regime="trending_up", eligible=True)


def _decision(qty: float = 1.0, *, stype: SignalType = SignalType.LONG,
              symbol: str = SYMBOL, sl_pct: float = 0.01,
              tp_pct: float = 0.02) -> RiskDecision:
    if stype is SignalType.SHORT:
        sl = ENTRY * (1 + sl_pct)
        tp = ENTRY * (1 - tp_pct)
    else:
        sl = ENTRY * (1 - sl_pct)
        tp = ENTRY * (1 + tp_pct)
    return RiskDecision(
        symbol=symbol, ts_ms=1, verdict=RiskVerdict.APPROVED.name,
        position_size=qty, stop_loss_price=sl, take_profit_price=tp,
        notional_value=qty * ENTRY, risk_amount=qty * ENTRY * 0.01,
        leverage_used=1, risk_checks={},
    )


async def _wait_until(pred, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.02)


class TestLifecycle:
    async def test_open_sequence_reaches_active(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            assert pos.status is PositionStatus.ACTIVE
            assert pos.quantity == pytest.approx(1.0)
            assert pos.entry_price == pytest.approx(ENTRY)
            resting = {r.client_order_id for r in await adapter.open_orders()}
            assert resting == {pos.stop_client_order_id, pos.take_profit_client_order_id}
            assert len(pm.open_positions()) == 1
        finally:
            await pm.close()

    async def test_short_signal_uses_sell_side(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(SignalType.SHORT),
                                decision=_decision(stype=SignalType.SHORT))
            assert pos.status is PositionStatus.ACTIVE
            assert pos.side == Side.SELL.name
            assert pos.stop_loss_price == pytest.approx(ENTRY * 1.01)
        finally:
            await pm.close()

    async def test_fill_happens_after_risk_approval(self):
        adapter, om, pm = _stack()
        await pm.start()
        adapter.set_price(SYMBOL, ENTRY)  # no-op, sanity
        sent_after = set(adapter._orders)
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            assert pos.entry_client_order_id in adapter._orders
            assert set(adapter._orders) - sent_after == {
                pos.entry_client_order_id,
                pos.stop_client_order_id,
                pos.take_profit_client_order_id,
            }
        finally:
            await pm.close()


class TestProtectionInvariant:
    async def test_never_active_without_protection(self):
        class _NoProtectionAdapter(SimulatedExecutionAdapter):
            async def submit(self, request: OrderRequest):
                if request.order_type == OrderType.STOP_MARKET.name:
                    raise OrderRejectedError("venue rejects stops")
                return await super().submit(request)

        adapter = _NoProtectionAdapter(_cfg(retries=1))
        adapter.set_price(SYMBOL, ENTRY)
        om = OrderManager(adapter, _cfg(retries=1))
        pm = PositionManager(om, _cfg(retries=1))
        await pm.start()
        try:
            with pytest.raises(PositionNotProtectedError):
                await pm.open(signal=_signal(), decision=_decision())
            # The protective market close ran -> no venue exposure remains.
            assert await adapter.open_positions() == ()
        finally:
            await pm.close()

    async def test_non_approved_decision_never_executes(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            bad = RiskDecision(symbol=SYMBOL, ts_ms=1,
                               verdict=RiskVerdict.REJECTED.name,
                               reason="RISK_EXPOSURE_EXCEEDED", risk_checks={})
            with pytest.raises(InvalidOrderError):
                await pm.open(signal=_signal(), decision=bad)
            assert await adapter.open_positions() == ()
            assert adapter._orders == {}
        finally:
            await pm.close()


class TestProtectionTriggers:
    async def test_stop_loss_hit_closes_position(self):
        adapter, om, pm = _stack()
        await pm.start()
        closed = []
        pm.on_position_closed(closed.append)
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            adapter.set_price(SYMBOL, ENTRY - 1500.0)  # below SL(49500) by 1000
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "stop_loss"
            assert pos.realized_pnl == pytest.approx(-1500.0)
            assert closed and closed[0] is pos
            assert pm.open_positions() == ()
            assert await adapter.open_positions() == ()
        finally:
            await pm.close()

    async def test_take_profit_hit_closes_position(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            adapter.set_price(SYMBOL, ENTRY + 2000.0)  # above TP(51000)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "take_profit"
            assert pos.realized_pnl == pytest.approx(2000.0)
        finally:
            await pm.close()

    async def test_short_stop_loss_pnl_negative(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(SignalType.SHORT),
                                decision=_decision(stype=SignalType.SHORT))
            adapter.set_price(SYMBOL, ENTRY + 1500.0)  # above short SL(50500)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "stop_loss"
            assert pos.realized_pnl == pytest.approx(-1500.0)
        finally:
            await pm.close()

    async def test_manual_close_cancels_protection(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            await pm.close_position(pos.position_id, reason="manual")
            assert pos.status is PositionStatus.CLOSED
            assert pos.close_reason == "manual"
            assert await adapter.open_orders() == ()
        finally:
            await pm.close()

    async def test_protection_lost_auto_closes(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            await adapter.cancel(SYMBOL, pos.stop_client_order_id)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "protection_lost"
            assert await adapter.open_orders() == ()
        finally:
            await pm.close()


class TestPartialEntry:
    async def test_partial_fill_protects_only_filled_qty(self):
        adapter, om, pm = _stack(_cfg(fill_timeout_s=0.15))
        await pm.start()
        try:
            entry_cid = "ENTRY-POS-PARTIAL001"
            adapter.schedule_partial(entry_cid, [0.4])
            pos = await pm.open(signal=_signal(), decision=_decision(qty=1.0),
                                position_id="pos-partial001")
            assert pos.entry_client_order_id == entry_cid
            assert pos.quantity == pytest.approx(0.4)
            assert pos.status is PositionStatus.ACTIVE
            resting = await adapter.open_orders()
            assert len(resting) == 2
            for r in resting:
                assert r.original_quantity == pytest.approx(0.4)
        finally:
            await pm.close()


class TestPortfolioRebuild:
    async def test_build_portfolio_state_reflects_pnl_and_losses(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            pos = await pm.open(signal=_signal(), decision=_decision())
            adapter.set_price(SYMBOL, ENTRY - 1000.0)  # below SL(49500)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            portfolio = pm.build_portfolio_state(
                equity=9900.0, initial_equity=10000.0, peak_equity=10000.0,
            )
            assert portfolio.open_count == 0
            assert portfolio.daily_realized_pnl == pytest.approx(-1000.0)
            assert portfolio.consecutive_losses == 1
            assert portfolio.total_risk_pct == 0.0
        finally:
            await pm.close()

    async def test_build_portfolio_state_includes_open_positions(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            await pm.open(signal=_signal(), decision=_decision(qty=2.0))
            await pm.open(signal=_signal(SignalType.SHORT),
                          decision=_decision(qty=1.0, stype=SignalType.SHORT))
            portfolio = pm.build_portfolio_state(equity=10000.0)
            assert portfolio.open_count == 2
            # risk_amounts (1000 + 500) / equity 10000
            assert portfolio.total_risk_pct == pytest.approx(0.15, abs=1e-3)
            assert SYMBOL in portfolio.symbols_open
        finally:
            await pm.close()

    async def test_open_requires_positive_size(self):
        adapter, om, pm = _stack()
        await pm.start()
        try:
            bad = _decision()
            bad = RiskDecision(
                symbol=SYMBOL, ts_ms=1, verdict=RiskVerdict.APPROVED.name,
                stop_loss_price=49500.0, take_profit_price=51000.0,
                risk_checks={},
            )
            with pytest.raises(InvalidOrderError):
                await pm.open(signal=_signal(), decision=bad)
            assert adapter._orders == {}
        finally:
            await pm.close()