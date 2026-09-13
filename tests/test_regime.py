"""Offline tests for MarketRegime classification."""

from crypto_scalper.core.enums import Regime
from crypto_scalper.features.regime import MarketRegime, RegimeThresholds


def reg(**kw):
    defaults = dict(
        price=100.0,
        ema9=100.0,
        ema21=100.0,
        ema50=100.0,
        adx=0.0,
        atr_pct=0.001,
        boll_upper=103.0,
        boll_lower=97.0,
    )
    defaults.update(kw)
    return MarketRegime().classify(**defaults)


class TestRegime:
    def test_trending_up(self):
        assert reg(ema9=101, ema21=100, ema50=99, adx=30) is Regime.TRENDING_UP

    def test_trending_down(self):
        assert reg(ema9=99, ema21=100, ema50=101, adx=30) is Regime.TRENDING_DOWN

    def test_range(self):
        assert reg(ema9=100, ema21=100, ema50=100, adx=10) is Regime.RANGE

    def test_breakout_above(self):
        assert reg(price=105, boll_upper=103) is Regime.BREAKOUT

    def test_extreme_vol(self):
        assert reg(atr_pct=0.02) is Regime.EXTREME

    def test_high_vol(self):
        thresholds = RegimeThresholds(atr_pct_high=0.0010)
        r = MarketRegime(thresholds)
        assert r.classify(
            price=100, ema9=100, ema21=100, ema50=100,
            adx=10, atr_pct=0.002, boll_upper=103, boll_lower=97,
        ) is Regime.HIGH_VOLATILITY

    def test_unknown(self):
        r = MarketRegime(RegimeThresholds(atr_pct_high=0.5, atr_pct_low=1e-6))
        assert r.classify(
            price=100, ema9=100, ema21=100, ema50=100,
            adx=22, atr_pct=0.001, boll_upper=103, boll_lower=97,
        ) is Regime.UNKNOWN