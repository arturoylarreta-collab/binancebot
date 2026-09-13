"""FASE 6 — ExecutionRouter: risk gates execution; approved flow works.

Verifies that REJECTED / TRADING_HALTED / SAFE_MODE decisions never result
in an order, that APPROVED decisions open a protected position, and that the
paper-oriented stack factory refuses LIVE.
"""

from __future__ import annotations

import dataclasses
import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.core.enums import PositionStatus, RiskVerdict, SignalType
from crypto_scalper.core.models import FeatureSnapshot, Signal
from crypto_scalper.execution.execution_router import ExecutionRouter, build_execution_stack
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter
from crypto_scalper.risk.portfolio import PortfolioState, Position
from crypto_scalper.risk.risk_engine import RiskEngine

SYMBOL = "BTCUSDT"
ENTRY = 50000.0


def _risk_config(**kw) -> RiskConfig:
    defaults = dict(
        risk_per_trade_pct=0.01,
        max_total_open_risk_pct=0.03,
        max_positions=10,
        max_leverage=1,
        daily_loss_limit_pct=0.03,
        max_drawdown_pct=0.10,
        correlated_group_exposure_cap_pct=0.06,
    )
    defaults.update(kw)
    return RiskConfig(**defaults)


def _stack(risk_config: RiskConfig | None = None):
    cfg = ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
    adapter = SimulatedExecutionAdapter(cfg)
    adapter.set_price(SYMBOL, ENTRY)
    om = OrderManager(adapter, cfg)
    pm = PositionManager(om, cfg)
    risk = RiskEngine(risk_config or _risk_config())
    router = ExecutionRouter(risk, pm, adapter)
    return adapter, om, pm, risk, router


def _portfolio(**kw) -> PortfolioState:
    defaults = dict(equity=10_000.0, initial_equity=10_000.0, peak_equity=10_000.0)
    defaults.update(kw)
    return PortfolioState(**defaults)


def _signal(stype: SignalType = SignalType.LONG) -> Signal:
    return Signal(symbol=SYMBOL, timestamp_ms=1, signal_type=stype, score=70.0,
                  regime="trending_up", eligible=True)


def _snapshot(atr: float = 500.0, price: float = ENTRY) -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol=SYMBOL, timestamp_ms=1, price=price, vwap=50200.0, rsi=60.0,
        atr=atr, atr_pct=atr / price, ema9=50100.0, ema21=50050.0, ema50=49900.0,
        adx=28.0, boll_upper=51000.0, boll_mid=50000.0, boll_lower=49000.0,
        roc=1.2, volume_zscore=1.5, relative_volume=1.3, buy_volume=10.0,
        sell_volume=8.0, trade_count=50, avg_trade_size=0.05, aggressive_volume=0.3,
        order_book_imbalance=0.1, microprice=50000.0, spread=1.0,
        spread_pct=0.00002, bid_depth=10.0, ask_depth=10.0, news_sentiment=0.0,
        news_impact="low", news_age_ms=5000, mention_zscore=0.2, regime="trending_up",
    )


async def _wait_until(pred, timeout: float = 2.0) -> None:
    import asyncio
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.02)


class TestRiskGatesExecution:
    async def test_rejected_never_submits(self):
        adapter, om, pm, risk, router = _stack(_risk_config(max_positions=1))
        await pm.start()
        try:
            existing = Position(symbol=SYMBOL, side="BUY", entry_price=50.0,
                                quantity=10.0, stop_loss_price=49.0,
                                take_profit_price=52.0, notional_value=500.0,
                                risk_amount=50.0, regime="trending_up")
            portfolio = _portfolio(open_positions=(existing,))
            outcome = await router.route(_signal(), _snapshot(), portfolio)
            assert outcome.submitted is False
            assert outcome.decision.verdict == RiskVerdict.REJECTED.name
            assert outcome.position is None
            assert adapter._orders == {}
        finally:
            await pm.close()

    async def test_halted_never_submits(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(),
                                         _portfolio(trading_halted=True))
            assert outcome.submitted is False
            assert outcome.decision.verdict == RiskVerdict.TRADING_HALTED.name
            assert adapter._orders == {}
        finally:
            await pm.close()

    async def test_safe_mode_never_submits(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(),
                                         _portfolio(safe_mode=True))
            assert outcome.submitted is False
            assert outcome.decision.verdict == RiskVerdict.SAFE_MODE.name
            assert adapter._orders == {}
        finally:
            await pm.close()

    async def test_missing_atr_rejected(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(atr=0.0), _portfolio())
            assert outcome.submitted is False
            assert outcome.decision.verdict != RiskVerdict.APPROVED.name
            assert adapter._orders == {}
        finally:
            await pm.close()


class TestApprovedFlow:
    async def test_approved_submits_and_protects(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(), _portfolio())
            assert outcome.submitted is True
            assert outcome.decision.verdict == RiskVerdict.APPROVED.name
            assert outcome.position is not None
            assert outcome.position.status is PositionStatus.ACTIVE
            assert len(pm.open_positions()) == 1
            resting = await adapter.open_orders()
            assert len(resting) == 2
        finally:
            await pm.close()

    async def test_position_size_driven_by_risk(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(atr=500.0), _portfolio())
            # risk_amount = 100 (1% of 10k); stop distance = 750 -> qty ~0.1333
            assert outcome.position.risk_amount == pytest.approx(100.0)
            assert outcome.position.notional_value == pytest.approx(
                outcome.position.quantity * ENTRY, rel=1e-3
            )
            assert outcome.position.quantity == pytest.approx(100.0 / 750.0, abs=4e-3)
        finally:
            await pm.close()

    async def test_price_move_triggers_stop_through_router(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(), _portfolio())
            pos = outcome.position
            router.update_price(SYMBOL, ENTRY - 2000.0)  # below SL
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "stop_loss"
            assert pos.realized_pnl < 0
        finally:
            await pm.close()

    async def test_portfolio_rebuilt_excludes_closed(self):
        adapter, om, pm, risk, router = _stack()
        await pm.start()
        try:
            outcome = await router.route(_signal(), _snapshot(), _portfolio())
            port = router.build_portfolio(10_000.0)
            assert port.open_count == 1
            router.update_price(SYMBOL, ENTRY + 5000.0)  # above TP
            await _wait_until(lambda: outcome.position.status is PositionStatus.CLOSED)
            port2 = router.build_portfolio(10_200.0)
            assert port2.open_count == 0
        finally:
            await pm.close()


class TestStackFactory:
    def test_paper_mode_builds_simulated_stack(self):
        settings = dataclasses.replace(Settings.load(), run_mode="paper")
        risk = RiskEngine(_risk_config())
        adapter, om, pm, router = build_execution_stack("paper", settings, risk)
        assert isinstance(adapter, SimulatedExecutionAdapter)
        assert om.adapter is adapter
        assert router.position_manager is pm

    def test_live_mode_refuses(self):
        settings = dataclasses.replace(Settings.load(), run_mode="live")
        risk = RiskEngine(_risk_config())
        with pytest.raises(NotImplementedError):
            build_execution_stack("live", settings, risk)