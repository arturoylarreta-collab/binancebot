"""FASE 5 — offline tests for the PositionSizer.

Verifies the risk-based sizing formula, leverage cap, tick/lot rounding,
zero/negative guards and determinism.
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.risk.position_sizing import PositionSizer


def _sizer(**kw) -> PositionSizer:
    defaults = dict(risk_per_trade_pct=0.01, max_leverage=3)
    defaults.update(kw)
    return PositionSizer(RiskConfig(**defaults))


class TestFormula:
    def test_size_equals_risk_amount_divided_by_stop_distance(self):
        s = _sizer(risk_per_trade_pct=0.01)
        res = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0)
        # risk_amount = 10000 * 0.01 = 100; stop_distance = 1.0
        assert res.risk_amount == pytest.approx(100.0)
        assert res.stop_distance == pytest.approx(1.0)
        assert res.quantity == pytest.approx(100.0)
        assert res.notional_value == pytest.approx(10_000.0)

    def test_notional_equals_quantity_times_price(self):
        s = _sizer()
        res = s.calculate(equity=50_000, entry_price=200.0, stop_price=199.0)
        assert res.notional_value == pytest.approx(res.quantity * 200.0)

    def test_larger_stop_distance_gives_smaller_position(self):
        s = _sizer(max_leverage=3)
        tight = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.5)
        wide = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0)
        assert tight.quantity > wide.quantity


class TestLeverage:
    def test_leverage_capped(self):
        s = _sizer(max_leverage=3)
        res = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0)
        assert res.leverage_used == 3
        # notional > equity is allowed up to 3x
        assert res.notional_value <= 30_000.0

    def test_notional_capped_at_leverage_times_equity(self):
        s = _sizer(max_leverage=2)
        res = s.calculate(equity=10_000, entry_price=100.0, stop_price=98.0)
        # raw notional would be risk 100 / 2.0 = 50 qty * 100 = 5000 < 20000
        assert res.capped is False
        # force cap: very tight stop -> huge raw quantity
        res2 = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.999)
        assert res2.capped is True
        assert res2.cap_reason == "leverage_cap"
        assert res2.notional_value <= 20_000.0

    def test_leverage_one_means_no_margin_cap_typically(self):
        s = _sizer(max_leverage=1)
        res = s.calculate(equity=10_000, entry_price=100.0, stop_price=98.0)
        assert res.leverage_used == 1
        assert res.notional_value <= 10_000.0


class TestRounding:
    def _grid_aligned(self, value: float, grid: float) -> bool:
        # value is a whole number of `grid` units (robust to binary floats)
        return abs(round(value / grid) - value / grid) < 1e-6

    def test_tick_size_floors_quantity(self):
        s = _sizer()
        res = s.calculate(
            equity=10_000, entry_price=100.0, stop_price=99.0,
            tick_size=0.001,
        )
        assert self._grid_aligned(res.quantity, 0.001)

    def test_lot_size_floors_quantity(self):
        s = _sizer()
        res = s.calculate(
            equity=10_000, entry_price=100.0, stop_price=99.0,
            lot_size=0.1,
        )
        assert self._grid_aligned(res.quantity, 0.1)


class TestEdgeCases:
    def test_zero_equity_rejected(self):
        s = _sizer()
        res = s.calculate(equity=0, entry_price=100.0, stop_price=99.0)
        assert res.quantity == 0.0
        assert res.capped is True
        assert res.cap_reason == "zero_equity"

    def test_negative_equity_rejected(self):
        s = _sizer()
        res = s.calculate(equity=-500, entry_price=100.0, stop_price=99.0)
        assert res.quantity == 0.0

    def test_zero_stop_distance_rejected(self):
        s = _sizer()
        res = s.calculate(equity=10_000, entry_price=100.0, stop_price=100.0)
        assert res.quantity == 0.0
        assert res.cap_reason == "zero_stop_distance"

    def test_deterministic(self):
        s = _sizer()
        a = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0)
        b = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0)
        assert a == b

    def test_valid_side_ignored_for_size(self):
        s = _sizer()
        a = s.calculate(equity=10_000, entry_price=100.0, stop_price=99.0, side="BUY")
        b = s.calculate(equity=10_000, entry_price=100.0, stop_price=101.0, side="SELL")
        assert a.quantity == pytest.approx(b.quantity)