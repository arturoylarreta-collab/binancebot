"""FASE 7 — PaperTradingEngine: smoke test with deterministic snapshot source."""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path
from typing import AsyncIterator

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.paper import PaperConfig
from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.paper.engine import PaperTradingEngine
from crypto_scalper.storage.sqlite_repo import SqliteRepository
from crypto_scalper.strategies.signal_engine import SignalEngine

SYMBOL = "BTCUSDT"
BASE_PRICE = 50000.0

_LONG_CATS = {
    "trend": {"trend_alignment": 0.8, "adx_strength": 0.6},
    "momentum": {"rsi": 65.0, "roc_10": 0.003, "roc_30": 0.02},
    "volume": {"volume_zscore": 1.0, "relative_volume": 2.0, "buy_ratio_30s": 0.6},
    "order_book": {"obi": 0.15, "obi_delta": 0.01, "spread_pct": 0.00002},
    "volatility": {"atr_pct": 0.01},
    "price": {"price_vs_vwap_pct": 0.001, "boll_position": 0.5, "return_60s": 0.001},
    "news": {"news_age_ms": 0, "news_ttl_ms": 0, "news_relevance": 0},
    "ml": {"ml_score": 50.0},
}


def _snapshot(price: float, ts_ms: int, *, long: bool = True) -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol=SYMBOL, timestamp_ms=ts_ms, price=price, vwap=price + 200.0,
        rsi=60.0, atr=500.0, atr_pct=500.0 / price, ema9=price - 100,
        ema21=price - 150, ema50=price - 200, adx=28.0, boll_upper=price * 1.02,
        boll_mid=price, boll_lower=price * 0.98, roc=1.2, volume_zscore=1.5,
        relative_volume=2.0, buy_volume=10.0, sell_volume=8.0, trade_count=50,
        avg_trade_size=0.05, aggressive_volume=0.3, order_book_imbalance=0.1,
        microprice=price, spread=1.0, spread_pct=0.00002, bid_depth=10.0,
        ask_depth=10.0, news_sentiment=0.0, news_impact="low", news_age_ms=5000,
        mention_zscore=0.2, regime="trending_up",
        features_by_category=_LONG_CATS if long else {},
    )


def _settings(**kw) -> Settings:
    defaults = dict(
        strategies=StrategyConfig(enabled=True),
        execution=ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0),
        risk=RiskConfig(
            risk_per_trade_pct=0.01, max_total_open_risk_pct=0.03,
            max_positions=10, max_leverage=1, daily_loss_limit_pct=0.03,
            max_drawdown_pct=0.10, correlated_group_exposure_cap_pct=0.06,
        ),
        paper=PaperConfig(
            start_equity=10_000.0, fee_pct=0.0,
            reconcile_interval_s=999.0, summary_interval_s=999.0,
            max_open_per_symbol=1, db_path=Path(":memory:"),
        ),
    )
    defaults.update(kw)
    return dataclasses.replace(Settings.load(), **defaults)


async def _iter(snapshots) -> AsyncIterator[FeatureSnapshot]:
    for s in snapshots:
        yield s


class TestDeterministicTrade:
    async def test_two_trades_then_cap(self):
        """Open → TP → open → SL → should stop at max_trades=2."""
        snapshots = [
            _snapshot(BASE_PRICE,            1000, long=True),   # open LONG
            _snapshot(BASE_PRICE + 2000.0,  2000, long=True),   # price 52000 → TP fill
            _snapshot(BASE_PRICE,            3000, long=True),   # re-open LONG
            _snapshot(BASE_PRICE - 2000.0,  4000, long=True),   # price 48000 → SL fill
            _snapshot(BASE_PRICE + 1000.0,  5000, long=True),   # should NOT be processed
        ]
        engine = PaperTradingEngine(
            settings=_settings(), signal_engine=SignalEngine(StrategyConfig(enabled=True)), source=_iter(snapshots))
        traded = await engine.run(max_trades=2)
        assert traded == 0  # run returns 0
        orch = engine.orchestrator
        assert orch.trades_closed == 2
        closed = orch.closed_positions()
        assert [p.close_reason for p in closed] == ["take_profit", "stop_loss"]
        assert closed[0].realized_pnl > 0.0
        assert closed[1].realized_pnl < 0.0
        assert engine.stopped


class TestNoTrades:
    async def test_neutral_source_trades_nothing(self):
        snaps = [_snapshot(BASE_PRICE + i, 1000 + i, long=False) for i in range(5)]
        engine = PaperTradingEngine(
            settings=_settings(), signal_engine=SignalEngine(StrategyConfig(enabled=True)), source=_iter(snaps))
        await engine.run()
        assert engine.orchestrator.trades_closed == 0
        assert engine.orchestrator.account.cash == pytest.approx(10_000.0)


class TestPersistence:
    async def test_repo_written_on_completion(self, tmp_path):
        snaps = [
            _snapshot(BASE_PRICE,      1000, long=True),
            _snapshot(BASE_PRICE + 2000.0, 2000, long=True),
        ]
        db = tmp_path / "paper.db"
        repo = SqliteRepository(db)
        settings = _settings(paper=PaperConfig(
            start_equity=10_000.0, fee_pct=0.0,
            reconcile_interval_s=999.0, summary_interval_s=999.0,
            max_open_per_symbol=1, db_path=db,
        ))
        engine = PaperTradingEngine(
            settings=settings,
            signal_engine=SignalEngine(StrategyConfig(enabled=True)),
            source=_iter(snaps), repository=repo)
        await engine.run(max_trades=1)
        # The engine closed the repository on exit; reopen the file for audit.
        reopened = SqliteRepository(db)
        assert reopened.count_rows("positions") >= 1
        assert reopened.count_rows("orders") >= 2  # entry + tp
        assert reopened.count_rows("fills") >= 2
        positions = await reopened.load_positions()
        assert positions[-1].close_reason == "take_profit"
        await reopened.close()


class TestStopEvent:
    async def test_stop_breaks_loop(self):
        engine = PaperTradingEngine(
            settings=_settings(), signal_engine=SignalEngine(StrategyConfig(enabled=True)), source=_iter([]))
        asyncio.get_running_loop().call_soon(engine.stop)
        result = await engine.run()
        assert result == 0
        assert engine.stopped