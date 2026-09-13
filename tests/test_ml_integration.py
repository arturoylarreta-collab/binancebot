"""FASE 4 — end-to-end integration:
FeatureEngine + MLPredictor + SignalEngine.

Covers: ml category injection into features_by_category, ml scorer reading
the model output (not the neutral 50 fallback), a deterministic stub
surrogate for speed, and no-ml fallback regression (FASE 3 contract).
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics
from crypto_scalper.strategies.signal_engine import SignalEngine

T0 = 1_700_000_000_000


class _DeterministicSurrogate:
    """Minimal predictor contract: (features_by_category) -> ml category dict."""

    def category(self, features_by_category):
        return {
            "p_up": 0.97,
            "p_down": 0.01,
            "p_neutral": 0.02,
            "ml_score": 97.0,
            "confidence": 0.95,
            "uncertainty": 0.05,
            "edge": 0.7,
        }


def _seed_state(drift=0.01, buy_frac=0.9) -> SymbolState:
    state = SymbolState("BTCUSDT")
    state.orderbook.apply_snapshot(
        100, [["99", "10"]], [["101", "10"]], ts_ms=T0
    )
    price = 100.0
    for sec in range(45):
        for k in range(5):
            is_buy = (k % 5) < (buy_frac * 5)
            from crypto_scalper.core.enums import AggressorSide
            from crypto_scalper.core.models import AggTrade

            price += drift if is_buy else -drift
            state.on_trade(
                AggTrade(
                    "BTCUSDT", T0 + sec * 1000 + k, sec * 5 + k, price, 1.0,
                    aggressor=AggressorSide.BUY if is_buy else AggressorSide.SELL,
                )
            )
    return state


def _engine(predictor=None) -> FeatureEngine:
    return FeatureEngine(FeatureConfig(), NewsStore(ttl_ms=60_000), Metrics(),
                         predictor=predictor)


def _snap(predictor=None) -> FeatureSnapshot:
    return _engine(predictor).compute(_seed_state(), now_ms=T0 + 45_500)


class TestMlInjection:
    def test_ml_category_present_with_predictor(self):
        snap = _snap(predictor=_DeterministicSurrogate())
        assert snap.features_by_category["ml"]["ml_score"] == pytest.approx(97.0)
        assert snap.features_by_category["ml"]["p_up"] == pytest.approx(0.97)

    def test_ml_scorer_uses_model_not_fallback(self):
        snap = _snap(predictor=_DeterministicSurrogate())
        sig = SignalEngine(StrategyConfig()).evaluate(snap)
        assert sig.components["ml"].score == pytest.approx(97.0)
        assert sig.components["ml"].score != 50.0

    def test_ml_component_raises_total_score(self):
        snap = _snap(predictor=_DeterministicSurrogate())
        sig = SignalEngine(StrategyConfig()).evaluate(snap)
        without_ml = SignalEngine(StrategyConfig()).evaluate(
            _snap(predictor=None)
        )
        assert sig.score >= without_ml.score  # greedy upsurge on an uptrend

    def test_ml_component_bounded_after_clip(self):
        snap = _snap(predictor=_DeterministicSurrogate())
        sig = SignalEngine(StrategyConfig()).evaluate(snap)
        assert 0.0 <= sig.components["ml"].score <= 100.0

    def test_no_predictor_falls_back_to_fifty(self):
        snap = _snap(predictor=None)
        assert "ml" not in snap.features_by_category
        sig = SignalEngine(StrategyConfig()).evaluate(snap)
        assert sig.components["ml"].score == 50.0


class TestRealModelEndToEnd:
    @pytest.mark.slow
    def test_trained_predictor_flows_through_signal_engine(self):
        from crypto_scalper.ml.features import build_dataset
        from crypto_scalper.ml.model_manager import ModelManager
        from crypto_scalper.ml.synthetic import SyntheticMarket

        snaps_tr = SyntheticMarket(
            symbols=("BTCUSDT", "ETHUSDT"), seconds_per_symbol=160, seed=9
        ).generate()
        ds = build_dataset(snaps_tr, horizon_s=30, threshold=0.0015)
        predictor, report = ModelManager(seed=9, n_windows=3).fit_best(
            ds, models=("logistic",)
        )
        # Re-time the same episodes through an engine wired to the model.
        snaps_pred = SyntheticMarket(
            symbols=("BTCUSDT", "ETHUSDT"),
            seconds_per_symbol=160,
            seed=9,
            predictor=predictor,
        ).generate()
        assert any("ml" in s.features_by_category for s in snaps_pred)
        sig = SignalEngine(StrategyConfig()).evaluate(snaps_pred[0])
        assert "ml" in sig.components
        assert 0.0 <= sig.components["ml"].score <= 100.0
        assert sig.components["ml"].score != pytest.approx(
            50.0, abs=1e-6
        ) or len({round(s.components["ml"].score, 4) for s in snaps_pred}) > 1