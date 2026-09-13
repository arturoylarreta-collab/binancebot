"""FASE 5 — offline tests for the ExposureManager.

Covers max positions, total risk cap, correlated group cap, and GPU-free
determinism (pure math).
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.risk.exposure import ExposureManager


def _manager(**kw) -> ExposureManager:
    defaults = dict(
        max_positions=3,
        max_total_open_risk_pct=0.03,
        correlated_group_exposure_cap_pct=0.06,
        correlation_groups=(("BTC", "ETH"),),
    )
    defaults.update(kw)
    return ExposureManager(RiskConfig(**defaults))


class TestMaxPositions:
    def test_allows_below_cap(self):
        m = _manager(max_positions=3)
        assert m.check_max_positions(2).passed is True

    def test_rejects_at_cap(self):
        m = _manager(max_positions=3)
        check = m.check_max_positions(3)
        assert check.passed is False
        assert "max_positions" in check.reason

    def test_rejects_above_cap(self):
        m = _manager(max_positions=3)
        assert m.check_max_positions(5).passed is False


class TestTotalRisk:
    def test_combined_under_cap_passes(self):
        m = _manager(max_total_open_risk_pct=0.03)
        assert m.check_total_risk(0.01, 0.01).passed is True

    def test_combined_over_cap_rejected(self):
        m = _manager(max_total_open_risk_pct=0.03)
        check = m.check_total_risk(0.025, 0.01)
        assert check.passed is False
        assert "total_risk" in check.reason

    def test_exact_cap_rejected(self):
        m = _manager(max_total_open_risk_pct=0.03)
        assert m.check_total_risk(0.02, 0.01).passed is False


class TestCorrelatedGroup:
    def test_no_group_no_constraint(self):
        m = _manager(correlation_groups=())
        assert m.check_correlated_group("BTCUSDT", 0.01, {}).passed is True

    def test_group_exposure_under_cap(self):
        m = _manager(correlation_groups=(("BTC", "ETH"),),
                     correlated_group_exposure_cap_pct=0.06)
        assert m.check_correlated_group("BTCUSDT", 0.03, {"BTC_ETH": 0.02}).passed is True

    def test_group_exposure_over_cap(self):
        m = _manager(correlation_groups=(("BTC", "ETH"),),
                     correlated_group_exposure_cap_pct=0.06)
        check = m.check_correlated_group("BTCUSDT", 0.03, {"BTC_ETH": 0.05})
        assert check.passed is False
        assert "correlated group" in check.reason

    def test_symbol_outside_group_does_not_share(self):
        m = _manager(correlation_groups=(("BTC", "ETH"),))
        assert m.check_correlated_group("SOLUSDT", 0.05, {"BTC_ETH": 0.05}).passed is True


class TestDeterminism:
    def test_repeatable(self):
        m = _manager()
        a = m.check_max_positions(3)
        b = m.check_max_positions(3)
        assert a == b