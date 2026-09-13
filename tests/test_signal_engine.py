"""FASE 3 — offline tests for the Signal Engine.

Covers: score bounds and weighted combination, per-dimension components,
regime gating, configurable weighting, validation of weights, direction
resolution (LONG/SHORT), and no look-ahead (expired/unavailable news is
neutral; the engine reads only the already-timestamped snapshot).
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.enums import AggressorSide, Impact, SignalType
from crypto_scalper.core.exceptions import ConfigurationError
from crypto_scalper.core.models import AggTrade, NewsEvent
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics
from crypto_scalper.strategies.signal_engine import SignalEngine

T0 = 1_700_000_000_000


def _seed_state(symbol="BTCUSDT", n_seconds=45, drift=0.01, buy_frac=0.6) -> SymbolState:
    state = SymbolState(symbol)
    state.orderbook.apply_snapshot(
        100,
        [["99", "10"], ["98", "5"]],
        [["101", "10"], ["102", "5"]],
        ts_ms=T0,
    )
    price = 100.0
    for sec in range(n_seconds):
        for k in range(5):
            is_buy = (k % 5) < (buy_frac * 5)
            price += drift if is_buy else -drift
            state.on_trade(
                AggTrade(
                    symbol, T0 + sec * 1000 + k, sec * 5 + k, price, 1.0,
                    aggressor=AggressorSide.BUY if is_buy else AggressorSide.SELL,
                )
            )
    return state


def _up_state():
    return _seed_state(n_seconds=45, drift=0.01, buy_frac=0.9)


def _down_state():
    state = SymbolState("BTCUSDT")
    state.orderbook.apply_snapshot(
        100,
        [["99", "10"], ["98", "5"]],
        [["101", "10"], ["102", "5"]],
        ts_ms=T0,
    )
    price = 100.0
    for sec in range(45):
        for k in range(5):
            price -= 0.01
            state.on_trade(
                AggTrade(
                    "BTCUSDT", T0 + sec * 1000 + k, sec * 5 + k, price, 1.0,
                    aggressor=AggressorSide.SELL,
                )
            )
    return state


def _snapshot(state, news=None, now_ms=None):
    store = NewsStore(ttl_ms=60_000)
    if news is not None:
        store.put(news)
    now = now_ms or T0 + 45_500
    return FeatureEngine(FeatureConfig(), store, Metrics()).compute(state, now_ms=now)


def _eval(snapshot, **kw):
    defaults = dict(enabled=True)
    defaults.update(kw)
    return SignalEngine(StrategyConfig(**defaults)).evaluate(snapshot)


class TestSignalScore:
    def test_score_bounds_and_components_bounds(self):
        sig = _eval(_snapshot(_up_state()))
        assert 0.0 <= sig.score <= 100.0
        assert sig.regime == "trending_up"
        for comp in sig.components.values():
            assert 0.0 <= comp.score <= 100.0
            assert comp.weight > 0.0

    def test_score_equals_weighted_combination(self):
        sig = _eval(_snapshot(_up_state()))
        expected = sum(c.score * c.weight for c in sig.components.values())
        assert sig.score == pytest.approx(expected, abs=1e-6)

    def test_all_default_weights_present(self):
        sig = _eval(_snapshot(_up_state()))
        assert set(sig.components) == {
            "trend", "momentum", "volume", "order_book",
            "volatility", "price_structure", "news", "ml",
        }

    def test_ml_dimension_always_neutral(self):
        sig = _eval(_snapshot(_up_state()))
        assert sig.components["ml"].score == 50.0

    def test_long_direction_in_uptrend(self):
        sig = _eval(_snapshot(_up_state()))
        assert sig.signal_type is SignalType.LONG
        assert sig.eligible is True

    def test_short_direction_in_downtrend(self):
        w = {"trend": 0.4, "momentum": 0.4, "price_structure": 0.2,
             "volume": 0.0, "order_book": 0.0, "volatility": 0.0,
             "news": 0.0, "ml": 0.0}
        sig = _eval(_snapshot(_down_state()), signal_weights=w)
        assert sig.score < 40.0
        assert sig.signal_type is SignalType.SHORT


class TestZeroWeights:
    def test_momentum_only_matches_component(self):
        w = {"trend": 0.0, "momentum": 1.0, "volume": 0.0, "order_book": 0.0,
             "volatility": 0.0, "price_structure": 0.0, "news": 0.0, "ml": 0.0}
        sig = _eval(_snapshot(_up_state()), signal_weights=w)
        assert sig.score == pytest.approx(sig.components["momentum"].score, abs=1e-6)

    def test_changing_weights_changes_score(self):
        snap = _snapshot(_seed_state(buy_frac=0.5, drift=0.01))
        mom_w = {"trend": 0.0, "momentum": 1.0, "volume": 0.0, "order_book": 0.0,
                 "volatility": 0.0, "price_structure": 0.0, "news": 0.0, "ml": 0.0}
        vol_w = {"trend": 0.0, "momentum": 0.0, "volume": 1.0, "order_book": 0.0,
                 "volatility": 0.0, "price_structure": 0.0, "news": 0.0, "ml": 0.0}
        s_mom = _eval(snap, signal_weights=mom_w).score
        s_vol = _eval(snap, signal_weights=vol_w).score
        assert s_mom >= 60.0  # strong drift -> momentumful
        assert s_mom != s_vol


class TestGating:
    def test_regime_blocked(self):
        sig = _eval(_snapshot(_up_state()), allowed_regimes=("range",))
        assert sig.eligible is False
        assert sig.signal_type is SignalType.FLAT
        assert sig.reason == "regime_blocked:trending_up"

    def test_regime_allowed(self):
        sig = _eval(_snapshot(_up_state()), allowed_regimes=("trending_up",))
        assert sig.eligible is True

    def test_empty_allowed_regimes_means_no_gating(self):
        sig = _eval(_snapshot(_up_state()), allowed_regimes=())
        assert sig.eligible is True

    def test_strategy_disabled(self):
        sig = _eval(_snapshot(_up_state()), enabled=False)
        assert sig.eligible is False
        assert sig.reason == "strategy_disabled"


class TestNews:
    def test_fresh_news_raises_component(self):
        news = NewsEvent(
            symbol="BTCUSDT", timestamp_ms=T0 + 1000, headline="great",
            source="test", sentiment=0.8, impact=Impact.HIGH, relevance=1.0,
        )
        snap = _snapshot(_up_state(), news=news, now_ms=T0 + 2000)
        sig = _eval(snap)
        assert sig.components["news"].score > 50.0

    def test_expired_news_is_neutral_no_lookahead(self):
        news = NewsEvent(
            symbol="BTCUSDT", timestamp_ms=T0 - 1_000_000, headline="old",
            source="test", sentiment=0.8, impact=Impact.HIGH, relevance=1.0,
        )
        snap = _snapshot(_up_state(), news=news, now_ms=T0 + 2000)
        sig = _eval(snap)
        assert sig.components["news"].score == 50.0


class TestValidation:
    def test_weights_must_sum_to_one(self):
        with pytest.raises(ConfigurationError):
            StrategyConfig(signal_weights={
                "trend": 0.4, "momentum": 0.4, "volume": 0.1, "order_book": 0.1,
                "volatility": 0.1, "price_structure": 0.1, "news": 0.1, "ml": 0.1,
            })

    def test_missing_dimension_rejected(self):
        with pytest.raises(ConfigurationError):
            StrategyConfig(signal_weights={"trend": 1.0})

    def test_threshold_order_invalid(self):
        with pytest.raises(ConfigurationError):
            StrategyConfig(short_threshold=70.0, long_threshold=60.0)

    def test_unknown_regime_rejected(self):
        with pytest.raises(ConfigurationError):
            StrategyConfig(allowed_regimes=("not_real",))