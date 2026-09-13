"""FASE 7 — PeriodicReconciler drift patrol and mitigation scenarios."""

from __future__ import annotations

import asyncio

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.enums import (
    OrderType,
    PositionStatus,
    RiskVerdict,
    SignalType,
)
from crypto_scalper.core.models import (
    FeatureSnapshot,
    ManagedPosition,
    OrderRequest,
    Signal,
)
from crypto_scalper.execution.execution_router import ExecutionRouter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.sqlite_repo import SqliteRepository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.trading.reconciler import PeriodicReconciler

SYMBOL = "BTCUSDT"
ENTRY = 50000.0


def _risk_config() -> RiskConfig:
    return RiskConfig(
        risk_per_trade_pct=0.01,
        max_total_open_risk_pct=0.03,
        max_positions=10,
        max_leverage=1,
        daily_loss_limit_pct=0.03,
        max_drawdown_pct=0.10,
        correlated_group_exposure_cap_pct=0.06,
    )


def _snapshot() -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol=SYMBOL, timestamp_ms=int(ENTRY), price=ENTRY, vwap=ENTRY + 200.0,
        rsi=60.0, atr=500.0, atr_pct=0.01, ema9=ENTRY - 100, ema21=ENTRY - 150,
        ema50=ENTRY - 200, adx=28.0, boll_upper=ENTRY * 1.02, boll_mid=ENTRY,
        boll_lower=ENTRY * 0.98, roc=1.2, volume_zscore=1.5, relative_volume=2.0,
        buy_volume=10.0, sell_volume=8.0, trade_count=50, avg_trade_size=0.05,
        aggressive_volume=0.3, order_book_imbalance=0.1, microprice=ENTRY,
        spread=1.0, spread_pct=0.00002, bid_depth=10.0, ask_depth=10.0,
        news_sentiment=0.0, news_impact="low", news_age_ms=5000,
        mention_zscore=0.2, regime="trending_up",
        features_by_category={},
    )


def _signal() -> Signal:
    return SignalEngine(StrategyConfig(enabled=True)).evaluate(_snapshot())


def _stack():
    cfg = ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
    adapter = SimulatedExecutionAdapter(cfg)
    adapter.set_price(SYMBOL, ENTRY)
    om = OrderManager(adapter, cfg)
    pm = PositionManager(om, cfg)
    router = ExecutionRouter(RiskEngine(_risk_config()), pm, adapter)
    return adapter, om, pm, router


async def _open_pm_position(pm, router) -> ManagedPosition:
    await pm.start()
    outcome = await router.route(
        _signal(), _snapshot(),
        PortfolioState(equity=10_000.0, initial_equity=10_000.0,
                       peak_equity=10_000.0))
    assert outcome.submitted, outcome.error
    return outcome.position


async def _wait_until(pred, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.02)


class TestCleanState:
    async def test_clean_reconcile_ok(self):
        adapter, om, pm, router = _stack()
        assert await _open_pm_position(pm, router)
        paused = []
        rec = PeriodicReconciler(pm, om, adapter, set_paused=paused.append)
        try:
            report = await rec.reconcile_once()
            assert report.ok is True
            assert report.fatal_issues == ()
            assert rec.reconcile_count == 1
            assert paused == []
        finally:
            await rec.close()
            await pm.close()


class TestMitigations:
    async def test_missing_stop_closes_active_position(self, monkeypatch):
        adapter, om, pm, router = _stack()
        pos = await _open_pm_position(pm, router)
        # Venue silently lost the stop-loss order; internal state is unaware.
        adapter._orders.pop(pos.stop_client_order_id, None)
        rec = PeriodicReconciler(pm, om, adapter)
        try:
            report = await rec.reconcile_once()
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "reconcile_missing_protection"
            assert report.ok is True
            assert rec.reconcile_count == 2  # detect + post-mitigation re-check
        finally:
            await rec.close()
            await pm.close()

    async def test_orphan_order_cancelled(self):
        adapter, om, pm, router = _stack()
        await _open_pm_position(pm, router)
        await adapter.submit(OrderRequest(
            symbol=SYMBOL, side="BUY", order_type=OrderType.LIMIT.name,
            quantity=1.0, price=ENTRY - 1000.0,
            client_order_id="ORPHAN-1",
            requested_ts_ms=1))
        rec = PeriodicReconciler(pm, om, adapter)
        try:
            report = await rec.reconcile_once()
            assert report.ok is True
            resting = {r.client_order_id for r in await adapter.open_orders()}
            assert "ORPHAN-1" not in resting
        finally:
            await rec.close()
            await pm.close()

    async def test_untracked_venue_position_flattened(self):
        adapter, om, pm, router = _stack()
        adapter._net_positions[SYMBOL] = 2.0  # venue holds a LONG nobody tracks
        rec = PeriodicReconciler(pm, om, adapter)
        try:
            report = await rec.reconcile_once()
            assert report.ok is True
            assert await adapter.open_positions() == ()
        finally:
            await rec.close()
            await pm.close()

    async def test_qty_mismatch_survives_mitigation_and_pauses(self):
        adapter, om, pm, router = _stack()
        await _open_pm_position(pm, router)
        adapter._net_positions[SYMBOL] = 0.5  # venue quantity drifted
        paused = []
        rec = PeriodicReconciler(pm, om, adapter, set_paused=paused.append)
        try:
            report = await rec.reconcile_once()
            assert report.fatal_issues
            assert any(i.kind == "QTY_MISMATCH" for i in report.fatal_issues)
            assert paused and "reconciliation_fatal" in paused[0]
            assert rec.last_report is report
        finally:
            await rec.close()
            await pm.close()


class TestCadenceAndAudit:
    async def test_interval_runs_periodically(self):
        adapter, om, pm, router = _stack()
        await _open_pm_position(pm, router)
        rec = PeriodicReconciler(pm, om, adapter, interval_s=0.05)
        try:
            await rec.start()
            await asyncio.sleep(0.16)
            assert rec.reconcile_count >= 2
        finally:
            await rec.close()
            await pm.close()

    async def test_reports_persisted_to_repository(self, tmp_path):
        adapter, om, pm, router = _stack()
        pos = await _open_pm_position(pm, router)
        adapter._orders.pop(pos.stop_client_order_id, None)
        repo = SqliteRepository(":memory:")
        rec = PeriodicReconciler(pm, om, adapter, repository=repo)
        try:
            await rec.reconcile_once()
            rows = repo.reconciliation_rows()
            assert rows and rows[-1][1] is True  # final post-mitigation report is ok
            assert repo.count_rows("reconciliations") == 1
        finally:
            await rec.close()
            await repo.close()
            await pm.close()