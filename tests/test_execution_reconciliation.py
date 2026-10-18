"""FASE 6 — ReconciliationEngine: internal state vs venue reality.

A healthy, protected position reconciles clean; missing stops, missing TPs,
quantity mismatches and orphan orders surface as typed issues; assert_clean()
raises ReconciliationMismatchError (Fatal) on any issue.
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderType
from crypto_scalper.core.exceptions import ReconciliationMismatchError
from crypto_scalper.core.models import OrderRequest
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.reconciliation import ReconciliationEngine
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter

SYMBOL = "BTCUSDT"
ENTRY = 50000.0


def _stack():
    cfg = ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
    adapter = SimulatedExecutionAdapter(cfg)
    adapter.set_price(SYMBOL, ENTRY)
    om = OrderManager(adapter, cfg)
    pm = PositionManager(om, cfg)
    return adapter, om, pm


from crypto_scalper.core.models import RiskDecision, Signal
from crypto_scalper.core.enums import RiskVerdict, SignalType


def _decision() -> RiskDecision:
    return RiskDecision(
        symbol=SYMBOL, ts_ms=1, verdict=RiskVerdict.APPROVED.name,
        position_size=1.0, stop_loss_price=ENTRY * 0.99,
        take_profit_price=ENTRY * 1.02, notional_value=ENTRY,
        risk_amount=100.0, leverage_used=1, risk_checks={},
    )


def _signal() -> Signal:
    return Signal(symbol=SYMBOL, timestamp_ms=1, signal_type=SignalType.LONG,
                  score=70.0, regime="trending_up", eligible=True)


async def _open_clean():
    adapter, om, pm = _stack()
    await pm.start()
    pos = await pm.open(signal=_signal(), decision=_decision())
    return adapter, om, pm, pos


class TestCleanState:
    async def test_healthy_protected_position_ok(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            assert report.ok is True
            assert report.issues == ()
        finally:
            await pm.close()


class TestMissingProtection:
    async def test_missing_stop_detected(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            await pm.close()  # stop the event consumer so cancel is not auto-handled
            await adapter.cancel(SYMBOL, pos.stop_client_order_id)
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            assert report.ok is False
            kinds = {i.kind for i in report.issues}
            assert "MISSING_STOP" in kinds
            with pytest.raises(ReconciliationMismatchError):
                ReconciliationEngine.assert_clean(report)
        finally:
            await pm.close()

    async def test_missing_tp_detected(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            await pm.close()
            await adapter.cancel(SYMBOL, pos.take_profit_client_order_id)
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            assert {"MISSING_TP"}.issubset({i.kind for i in report.issues})
        finally:
            await pm.close()


class TestMismatches:
    async def test_quantity_mismatch_detected(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            # Extra venue exposure the internal book does not know about.
            await adapter.submit(OrderRequest(
                symbol=SYMBOL, side="BUY", order_type=OrderType.MARKET.name,
                quantity=1.0, client_order_id="ghost", requested_ts_ms=1,
            ))
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            assert {"QTY_MISMATCH"}.issubset({i.kind for i in report.issues})
        finally:
            await pm.close()

    async def test_orphan_order_is_warning(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            await om.submit(OrderRequest(
                symbol=SYMBOL, side="BUY", order_type=OrderType.LIMIT.name,
                quantity=0.5, price=40000.0, client_order_id="TP-LONE",
                requested_ts_ms=1,
            ))
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            kinds = {i.kind for i in report.issues}
            assert "ORPHAN_ORDER" in kinds
            assert report.fatal_issues == ()
        finally:
            await pm.close()

    async def test_assert_clean_raises_on_any_issue(self):
        adapter, om, pm, pos = await _open_clean()
        try:
            await pm.close()
            await adapter.cancel(SYMBOL, pos.stop_client_order_id)
            report = await ReconciliationEngine().reconcile(pm, om, adapter)
            with pytest.raises(ReconciliationMismatchError):
                ReconciliationEngine.assert_clean(report)
        finally:
            await pm.close()