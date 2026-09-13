"""FASE 5 — offline tests for the TakeProfitCalculator.

Covers risk:reward math, directionality, floor on the ratio, and
degenerate inputs.
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.risk.take_profit import TakeProfitCalculator


def _tp(**kw) -> TakeProfitCalculator:
    defaults = dict(take_profit_enabled=True, min_cost_edge_ratio=1.5)
    defaults.update(kw)
    return TakeProfitCalculator(RiskConfig(**defaults))


class TestTakeProfit:
    def test_long_tp_above_entry(self):
        calc = _tp()
        res = calc.calculate(entry_price=100.0, stop_price=99.0, side="BUY", rr_ratio=2.0)
        # stop_distance = 1.0; tp_distance = 2.0
        assert res.take_profit_price == pytest.approx(102.0)
        assert res.tp_distance == pytest.approx(2.0)
        assert res.rr_ratio == pytest.approx(2.0)

    def test_short_tp_below_entry(self):
        calc = _tp()
        res = calc.calculate(entry_price=100.0, stop_price=101.0, side="SELL", rr_ratio=3.0)
        assert res.take_profit_price == pytest.approx(97.0)

    def test_rr_ratio_floored(self):
        calc = _tp(min_cost_edge_ratio=1.5)
        res = calc.calculate(entry_price=100.0, stop_price=99.0, side="BUY", rr_ratio=1.0)
        assert res.rr_ratio == pytest.approx(1.5)

    def test_rr_ratio_above_floor_kept(self):
        calc = _tp(min_cost_edge_ratio=1.5)
        res = calc.calculate(entry_price=100.0, stop_price=99.0, side="BUY", rr_ratio=2.5)
        assert res.rr_ratio == pytest.approx(2.5)

    def test_higher_rr_gives_farther_tp(self):
        calc = _tp()
        low = calc.calculate(entry_price=100.0, stop_price=99.0, rr_ratio=1.5)
        high = calc.calculate(entry_price=100.0, stop_price=99.0, rr_ratio=4.0)
        assert high.tp_distance > low.tp_distance

    def test_deterministic(self):
        calc = _tp()
        a = calc.calculate(entry_price=100.0, stop_price=99.0)
        b = calc.calculate(entry_price=100.0, stop_price=99.0)
        assert a == b

    def test_zero_stop_distance_gives_zero_tp_distance(self):
        calc = _tp()
        res = calc.calculate(entry_price=100.0, stop_price=100.0)
        assert res.take_profit_price == pytest.approx(100.0)
        assert res.tp_distance == pytest.approx(0.0)