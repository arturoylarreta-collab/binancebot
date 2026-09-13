"""FASE 3 — offline tests for the per-category feature bundles.

Verifies the Feature Engine produces all ten categories with scalar,
timestamped values, and that decision features depend only on data
available at the decision instant (no look-ahead).
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.core.enums import AggressorSide, Impact
from crypto_scalper.core.models import AggTrade, NewsEvent
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features import categories as cat
from crypto_scalper.features import technicals as t
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

T0 = 1_700_000_000_000


def _seed_state(symbol="BTCUSDT", n_seconds=40, drift=0.01, buy_frac=0.6) -> SymbolState:
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


def _dt(n: int) -> int:
    return T0 + n * 1000 + 500


def _snap(state, now_ms=None, news=None):
    store = NewsStore(ttl_ms=60_000)
    if news is not None:
        store.put(news)
    now = now_ms or _dt(41)
    return FeatureEngine(FeatureConfig(), store, Metrics()).compute(state, now_ms=now)


class TestFeatureCategories:
    def test_all_ten_categories_present(self):
        snap = _snap(_seed_state())
        assert set(snap.features_by_category.keys()) == set(cat.CATEGORY_KEYS)

    def test_values_are_scalars(self):
        snap = _snap(_seed_state())
        for category in snap.features_by_category.values():
            for value in category.values():
                assert isinstance(value, (int, float, str))

    def test_price_category_structure(self):
        pc = _snap(_seed_state()).features_by_category["price"]
        for key in ("price", "return_1s", "roc_10", "price_vs_ema9_pct",
                    "price_vs_vwap_pct", "boll_position"):
            assert key in pc

    def test_volume_category_keys(self):
        vc = _snap(_seed_state()).features_by_category["volume"]
        assert {"volume_zscore", "relative_volume", "buy_ratio_30s", "trade_count_30s"} <= set(vc)

    def test_order_flow_buy_bias(self):
        of = _snap(_seed_state(buy_frac=0.9, drift=0.01)).features_by_category["order_flow"]
        assert of["flow_imbalance_30s"] > 0
        assert of["candle_delta_30s"] > 0
        assert of["aggressive_buy_ratio_30s"] > 0.5

    def test_momentum_uptrend(self):
        mom = _snap(_seed_state(buy_frac=0.9, drift=0.01)).features_by_category["momentum"]
        assert mom["roc_10"] > 0
        assert mom["rsi"] > 50

    def test_trend_alignment_up(self):
        trend = _snap(_seed_state(drift=0.01, buy_frac=0.9)).features_by_category["trend"]
        assert trend["trend_alignment"] == 1.0
        assert trend["adx_strength"] > 0.5

    def test_order_book_category(self):
        obc = _snap(_seed_state()).features_by_category["order_book"]
        for key in ("obi", "obi_delta", "spread_pct", "bid_ask_ratio",
                    "depth_pct0005", "depth_pct0010", "depth_pct0025"):
            assert key in obc
        assert obc["obi_delta"] == 0.0  # first evaluation

    def test_news_category_fresh(self):
        news = NewsEvent(
            symbol="BTCUSDT", timestamp_ms=T0 + 1000, headline="strong",
            source="test", sentiment=0.9, impact=Impact.HIGH, relevance=1.0,
        )
        snap = _snap(_seed_state(), now_ms=T0 + 2000, news=news)
        nc = snap.features_by_category["news"]
        assert nc["news_sentiment"] == 0.9
        assert nc["news_impact_weight"] == 1.0
        assert nc["news_relevance"] == 1.0
        assert nc["news_age_ms"] >= 0

    def test_market_regime_category_flags(self):
        snap = _snap(_seed_state(drift=0.01, buy_frac=0.9))
        rc = snap.features_by_category["market_regime"]
        assert rc["regime_name"] == snap.regime
        assert rc["is_trending"] in (0.0, 1.0)
        assert rc["is_unknown"] in (0.0, 1.0)

    def test_decision_uses_only_data_available_at_decision_time(self):
        state = _seed_state(n_seconds=40)
        dtime = _dt(39)
        snap = _snap(state, now_ms=dtime)
        s = state.candles.series()
        mask = s.ts <= dtime
        close, high, low = s.close[mask], s.high[mask], s.low[mask]
        typical = t.typical_price(high, low, close)
        assert abs(t.ema(close, 9) - snap.ema9) < 1e-6
        assert abs(t.ema(close, 21) - snap.ema21) < 1e-6
        assert abs(t.rsi(close, 14) - snap.rsi) < 1e-6
        assert abs(t.roc(close, 10) - snap.roc) < 1e-6
        assert abs(t.session_vwap(s.ts[mask], typical, s.volume[mask]) - snap.vwap) < 1e-6

    def test_features_are_bound_to_decision_timestamp(self):
        state = _seed_state(n_seconds=20)
        early = _snap(state, now_ms=T0 + 20_500)
        late = _snap(state, now_ms=T0 + 400_000)
        assert early.features_by_category["volume"]["trade_count_30s"] > 0
        assert late.features_by_category["volume"]["trade_count_30s"] == 0