"""FASE 8 — BacktestEngine end-to-end against a synthetic deterministic corpus."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from crypto_scalper.backtest.data import KlineCorpus
from crypto_scalper.backtest.engine import BacktestEngine
from crypto_scalper.config.backtest import BacktestConfig
from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.models import Candle
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.strategies.signal_engine import SignalEngine


def _settings(**kw) -> Settings:
    defaults = {
        "strategies": StrategyConfig(enabled=True),
        "execution": ExecutionConfig(
            retries=0, backoff_base_s=0.01, slippage_pct=0.0005,
        ),
        "backtest": BacktestConfig(
            data_dir=Path("data/klines"), interval_s=60, warmup_bars=30,
            fill_at="close", start_equity=10_000.0,
        ),
    }
    defaults.update(kw)
    return dataclasses.replace(Settings.load(), **defaults)


def _rising_corpus(n_bars=180, start=50_000.0, drift=0.004, interval_s=60) -> KlineCorpus:
    candles = []
    price = start
    base_ts = 1_700_000_000_000
    for i in range(n_bars):
        close = price * (1.0 + drift)
        high = max(price, close) * 1.001
        low = min(price, close) * 0.999
        candles.append(Candle(
            symbol="BTCUSDT", ts_ms=base_ts + i * interval_s * 1000,
            interval_s=interval_s, open=price, high=high, low=low, close=close,
            volume=5.0, quote_volume=close * 5.0, trade_count=50, completed=True,
        ))
        price = close
    return KlineCorpus({"BTCUSDT": candles})


class TestBacktestEngine:
    async def test_runs_and_produces_report(self):
        settings = _settings()
        engine = BacktestEngine(
            settings=settings,
            corpus=_rising_corpus(),
            feature_engine=FeatureEngine(settings.features, NewsStore(), METRICS),
            signal_engine=SignalEngine(settings.strategies),
        )
        report = await engine.run(max_trades=2)
        assert report.stats["initial_equity"] == 10_000.0
        assert report.stats["trades"] >= 0
        assert len(report.equity_curve) >= 1
        text = report.summary_text()
        assert "Backtest" in text

    async def test_next_open_fill_mode_runs(self):
        bt = dataclasses.replace(BacktestConfig(), fill_at="next_open", warmup_bars=30)
        settings = _settings(backtest=bt)
        engine = BacktestEngine(
            settings=settings,
            corpus=_rising_corpus(n_bars=60),
            feature_engine=FeatureEngine(settings.features, NewsStore(), METRICS),
            signal_engine=SignalEngine(settings.strategies),
        )
        report = await engine.run(max_trades=1)
        assert report.stats["trades"] >= 0

    async def test_partial_entry_fraction_runs(self):
        bt = dataclasses.replace(
            BacktestConfig(), warmup_bars=30, entry_fill_fraction=0.8,
        )
        settings = _settings(backtest=bt)
        engine = BacktestEngine(
            settings=settings,
            corpus=_rising_corpus(n_bars=60),
            feature_engine=FeatureEngine(settings.features, NewsStore(), METRICS),
            signal_engine=SignalEngine(settings.strategies),
        )
        await engine.run(max_trades=1)

    async def test_independent_runs_are_deterministic(self):
        def build():
            settings = _settings()
            return BacktestEngine(
                settings=settings,
                corpus=_rising_corpus(n_bars=90),
                feature_engine=FeatureEngine(settings.features, NewsStore(), METRICS),
                signal_engine=SignalEngine(settings.strategies),
            )
        r1 = await build().run(max_trades=1)
        r2 = await build().run(max_trades=1)
        assert r1.to_dict() == r2.to_dict()