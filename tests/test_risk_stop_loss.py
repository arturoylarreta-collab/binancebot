"""FASE 5 — offline tests for the StopLossCalculator.

Covers ATR-based and fixed-percentage stops, min/max clamping,
directionality (long vs short), and degenerate inputs.
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.risk.stop_loss import StopLossCalculator


def _sl(**kw) -> StopLossCalculator:
    defaults = dict(stop_loss_enabled=True)
    defaults.update(kw)
    return StopLossCalculator(RiskConfig(**defaults))


class TestAtrBased:
    def test_long_stop_below_entry(self):
        calc = _sl()
        res = calc.atr_based(entry_price=100.0, atr=1.0, side="BUY", atr_mult=1.5)
        assert res.stop_price == pytest.approx(100.0 - 1.5)
        assert res.stop_distance == pytest.approx(1.5)
        assert res.method == "atr"

    def test_short_stop_above_entry(self):
        calc = _sl()
        res = calc.atr_based(entry_price=100.0, atr=1.0, side="SELL", atr_mult=1.5)
        assert res.stop_price == pytest.approx(100.0 + 1.5)

    def test_multiplier_scales_distance(self):
        calc = _sl()
        a = calc.atr_based(entry_price=100.0, atr=1.0, atr_mult=1.0)
        b = calc.atr_based(entry_price=100.0, atr=1.0, atr_mult=2.0)
        assert b.stop_distance == pytest.approx(a.stop_distance * 2.0)

    def test_zero_atr_invalid(self):
        calc = _sl()
        res = calc.atr_based(entry_price=100.0, atr=0.0)
        assert res.stop_price == 0.0
        assert res.method == "invalid"


class TestFixedPct:
    def test_long_fixed_pct(self):
        calc = _sl()
        res = calc.fixed_pct(entry_price=200.0, side="BUY", pct=0.005)
        assert res.stop_price == pytest.approx(200.0 * (1 - 0.005))
        assert res.method == "fixed_pct"

    def test_short_fixed_pct(self):
        calc = _sl()
        res = calc.fixed_pct(entry_price=200.0, side="SELL", pct=0.005)
        assert res.stop_price == pytest.approx(200.0 * (1 + 0.005))

    def test_invalid_pct(self):
        calc = _sl()
        res = calc.fixed_pct(entry_price=200.0, pct=0.0)
        assert res.method == "invalid"


class TestClamping:
    def test_min_clamp_no_absurd_tight_stop(self):
        calc = _sl()
        # atr = 0.0001 -> distance = 0.00015 -> below min (100*0.0005=0.05)
        res = calc.atr_based(entry_price=100.0, atr=0.0001, atr_mult=1.5)
        assert res.stop_distance >= 100.0 * 0.0005

    def test_max_clamp_no_absurd_wide_stop(self):
        calc = _sl()
        # atr = 10 -> distance clamped to 100 * 0.05 = 5.0
        res = calc.atr_based(entry_price=100.0, atr=10.0, atr_mult=2.0)
        assert res.stop_distance <= 100.0 * 0.05

    def test_deterministic(self):
        calc = _sl()
        a = calc.atr_based(entry_price=100.0, atr=1.0)
        b = calc.atr_based(entry_price=100.0, atr=1.0)
        assert a == b