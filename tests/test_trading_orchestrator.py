"""FASE 7 — TradeOrchestrator: full offline trading loop over the venue."""

from __future__ import annotations

import asyncio

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.paper import PaperConfig
from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.enums import PositionStatus, RiskVerdict
from crypto_scalper.core.exceptions import ExecutionError
from crypto_scalper.core.models import FeatureSnapshot, Signal
from crypto_scalper.execution.execution_router import ExecutionRouter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.base import NoopRepository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.trading.account import PaperAccount
from crypto_scalper.trading.orchestrator import TradeOrchestrator

SYMBOL = "BTCUSDT"
SYMBOL2 = "ETHUSDT"
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


def _strategy() -> StrategyConfig:
    return StrategyConfig(enabled=True)


def _categories(*, kind: str = "long") -> dict:
    if kind == "long":
        return {
            "trend": {"trend_alignment": 0.8, "adx_strength": 0.6},
            "momentum": {"rsi": 65.0, "roc_10": 0.003, "roc_30": 0.02},
            "volume": {"volume_zscore": 1.0, "relative_volume": 2.0, "buy_ratio_30s": 0.6},
            "order_book": {"obi": 0.15, "obi_delta": 0.01, "spread_pct": 0.00002},
            "volatility": {"atr_pct": 0.01},
            "price": {"price_vs_vwap_pct": 0.001, "boll_position": 0.5, "return_60s": 0.001},
            "news": {"news_age_ms": 0, "news_ttl_ms": 0, "news_relevance": 0},
            "ml": {"ml_score": 50.0},
        }
    if kind == "short":
        return {
            "trend": {"trend_alignment": -0.8, "adx_strength": 0.6},
            "momentum": {"rsi": 35.0, "roc_10": -0.005, "roc_30": -0.03},
            "volume": {"volume_zscore": -1.0, "relative_volume": 2.0, "buy_ratio_30s": 0.4},
            "order_book": {"obi": -0.15, "obi_delta": -0.01, "spread_pct": 0.00002},
            "volatility": {"atr_pct": 0.0002},
            "price": {"price_vs_vwap_pct": -0.001, "boll_position": -0.8, "return_60s": -0.002},
            "news": {"news_age_ms": 0, "news_ttl_ms": 0, "news_relevance": 0},
            "ml": {"ml_score": 50.0},
        }
    return {
        "trend": {"trend_alignment": 0.0, "adx_strength": 0.2},
        "momentum": {"rsi": 50.0, "roc_10": 0.0, "roc_30": 0.0},
        "volume": {"volume_zscore": 0.0, "relative_volume": 1.0, "buy_ratio_30s": 0.5},
        "order_book": {"obi": 0.0, "obi_delta": 0.0, "spread_pct": 0.00005},
        "volatility": {"atr_pct": 0.005},
        "price": {"price_vs_vwap_pct": 0.0, "boll_position": 0.0, "return_60s": 0.0},
        "news": {"news_age_ms": 0, "news_ttl_ms": 0, "news_relevance": 0},
        "ml": {"ml_score": 50.0},
    }


def _snapshot(price: float = ENTRY, symbol: str = SYMBOL, *,
              atr: float = 500.0, kind: str = "long") -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol=symbol, timestamp_ms=int(price), price=price, vwap=price + 200.0,
        rsi=60.0, atr=atr, atr_pct=atr / price, ema9=price - 100, ema21=price - 150,
        ema50=price - 200, adx=28.0, boll_upper=price * 1.02, boll_mid=price,
        boll_lower=price * 0.98, roc=1.2, volume_zscore=1.5, relative_volume=2.0,
        buy_volume=10.0, sell_volume=8.0, trade_count=50, avg_trade_size=0.05,
        aggressive_volume=0.3, order_book_imbalance=0.1, microprice=price, spread=1.0,
        spread_pct=0.00002, bid_depth=10.0, ask_depth=10.0, news_sentiment=0.0,
        news_impact="low", news_age_ms=5000, mention_zscore=0.2, regime="trending_up",
        features_by_category=_categories(kind=kind),
    )


def _stack(risk_config=None, *, fee_pct=0.0, paper_config=None):
    cfg = ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
    adapter = SimulatedExecutionAdapter(cfg)
    adapter.set_price(SYMBOL, ENTRY)
    om = OrderManager(adapter, cfg)
    pm = PositionManager(om, cfg)
    risk = RiskEngine(risk_config or _risk_config())
    router = ExecutionRouter(risk, pm, adapter)
    account = PaperAccount(10_000.0, fee_pct)
    sig = SignalEngine(_strategy())
    orch = TradeOrchestrator(
        router, sig, account, NoopRepository(), order_manager=om,
        config=paper_config or PaperConfig(),
    )
    return adapter, om, pm, router, account, orch


async def _wait_until(pred, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.02)


class TestApproveAndReject:
    async def test_long_signal_opens_protected_position(self):
        _, _, pm, _, _, orch = _stack()
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            assert outcome.submitted is True
            assert outcome.position.status is PositionStatus.ACTIVE
            assert len(pm.open_positions()) == 1
        finally:
            await orch.close()

    async def test_rejected_by_risk_never_submits(self):
        adapter, _, pm, _, _, orch = _stack(
            _risk_config(max_positions=1), fee_pct=0.0)
        await orch.start()
        try:
            first = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            assert first.submitted is True
            # Second symbol would burst max_positions -> rejected, no order.
            adapter.set_price(SYMBOL2, ENTRY)
            snap2 = _snapshot(symbol=SYMBOL2)
            second = await orch.on_signal(
                orch._signals.evaluate(snap2), snap2)
            assert second.submitted is False
            assert second.decision.verdict == RiskVerdict.REJECTED.name
            assert adapter._orders.get("ENTRY-ETHUSDT") is None
            assert len(pm.open_positions()) == 1
        finally:
            await orch.close()

    async def test_symbol_guard_blocks_second_open_same_symbol(self):
        adapter, _, pm, _, _, orch = _stack()
        await orch.start()
        try:
            first = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            assert first.submitted is True
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot(price=ENTRY + 1)),
                _snapshot(price=ENTRY + 1))
            assert outcome.submitted is False
            assert "symbol_already_open" in outcome.error
            assert len(pm.open_positions()) == 1
        finally:
            await orch.close()

    async def test_zero_atr_rejected(self):
        adapter, _, pm, _, _, orch = _stack()
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot(atr=0.0)),
                _snapshot(atr=0.0))
            assert outcome.submitted is False
            assert adapter._orders == {}
            assert pm.open_positions() == ()
        finally:
            await orch.close()

    async def test_neutral_signal_not_eligible(self):
        adapter, _, pm, _, _, orch = _stack()
        await orch.start()
        try:
            neutral = _snapshot(kind="neutral")
            signal = orch._signals.evaluate(neutral)
            assert signal.signal_type.name == "FLAT"
            outcome = await orch.on_signal(signal, neutral)
            assert outcome.submitted is False
            assert adapter._orders == {}
        finally:
            await orch.close()


class TestPriceLoopAndPnl:
    async def test_take_profit_close_updates_account(self):
        _, _, _, _, account, orch = _stack()
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            pos = outcome.position
            await orch.on_price(SYMBOL, ENTRY + 2000.0)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "take_profit"
            assert orch.trades_closed == 1
            assert account.realized_pnl > 0
            assert account.cash > 10_000.0
            summary = orch.summary()
            assert summary.equity > 10_000.0
        finally:
            await orch.close()

    async def test_stop_loss_close_realizes_loss(self):
        _, _, _, _, account, orch = _stack()
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            pos = outcome.position
            await orch.on_price(SYMBOL, ENTRY - 2000.0)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "stop_loss"
            assert account.realized_pnl < 0
            assert orch.summary().consecutive_losses >= 1
        finally:
            await orch.close()

    async def test_short_take_profit_close(self):
        _, _, _, _, account, orch = _stack()
        await orch.start()
        try:
            snap = _snapshot(kind="short")
            outcome = await orch.on_signal(orch._signals.evaluate(snap), snap)
            assert outcome.submitted is True
            assert outcome.position.side == "SELL"
            pos = outcome.position
            await orch.on_price(SYMBOL, ENTRY - 2000.0)  # below short TP(48000)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "take_profit"
            assert account.realized_pnl > 0
        finally:
            await orch.close()

    async def test_fees_reduce_cash(self):
        _, _, _, _, account, orch = _stack(fee_pct=0.001)
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            pos = outcome.position
            await orch.on_price(SYMBOL, ENTRY + 2000.0)
            await _wait_until(lambda: pos.status is PositionStatus.CLOSED)
            assert account.fees_paid > 0
            stats = account.stats()
            assert stats.equity < 10_000.0 + account.realized_pnl
        finally:
            await orch.close()

    async def test_equity_reflected_in_portfolio(self):
        _, _, _, router, _, orch = _stack()
        await orch.start()
        try:
            await orch.on_signal(orch._signals.evaluate(_snapshot()), _snapshot())
            portfolio = orch.build_portfolio()
            assert portfolio.equity == pytest.approx(10_000.0)
            assert router.build_portfolio(orch.build_portfolio().equity) is not None
        finally:
            await orch.close()


class TestFatalAndPause:
    async def test_fatal_execution_error_pauses(self, monkeypatch):
        _, _, _, router, _, orch = _stack()
        adapter = router.adapter
        orig = adapter.submit

        async def broken_submit(request):
            if request.order_type == "STOP_MARKET":
                raise ExecutionError("protection failure")
            return await orig(request)

        monkeypatch.setattr(adapter, "submit", broken_submit)
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            assert outcome.submitted is False
            assert "PositionNotProtectedError" in outcome.error
            assert orch.paused is True
            assert orch.trades_closed == 1  # abort close realized a loss
            # Everything after the fatal is blocked without touching the venue.
            outcome2 = await orch.on_signal(
                orch._signals.evaluate(_snapshot(price=ENTRY + 1)),
                _snapshot(price=ENTRY + 1))
            assert outcome2.submitted is False
            assert "orchestrator_paused" in outcome2.error
        finally:
            await orch.close()

    async def test_paused_block_keeps_summary(self):
        _, _, _, _, account, orch = _stack()
        orch._pause("explicit")
        assert orch.paused is True
        assert orch.pause_reason == "explicit"
        snap = _snapshot()
        outcome = await orch.on_signal(orch._signals.evaluate(snap), snap)
        assert outcome.submitted is False
        assert "orchestrator_paused" in outcome.error
        assert orch.summary().paused is True


class TestPersistence:
    async def test_persist_writes_positions_orders_fills(self):
        _, _, pm, _, _, orch = _stack()
        await orch.start()
        try:
            outcome = await orch.on_signal(
                orch._signals.evaluate(_snapshot()), _snapshot())
            await orch.on_price(SYMBOL, ENTRY + 2000.0)
            await _wait_until(lambda: outcome.position.status is PositionStatus.CLOSED)
            await orch.persist()
            assert pm.positions() != ()
            # Noop repo swallows everything without raising.
        finally:
            await orch.close()