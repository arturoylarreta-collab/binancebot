"""FASE 5 — offline tests for PortfolioState.

Covers equity/drawdown math, total risk, correlated-group exposures,
and immutability conventions.
"""

from __future__ import annotations

import pytest

from crypto_scalper.risk.portfolio import PortfolioState, Position


def _pos(symbol="BTCUSDT", side="BUY", entry=100.0, qty=10.0, risk=50.0,
          regime="trending_up") -> Position:
    return Position(
        symbol=symbol, side=side, entry_price=entry, quantity=qty,
        stop_loss_price=97.0, take_profit_price=103.0,
        notional_value=entry * qty, risk_amount=risk, opened_ts_ms=0,
        regime=regime,
    )


def _portfolio(**kw) -> PortfolioState:
    defaults = dict(
        equity=10_000.0,
        initial_equity=10_000.0,
        peak_equity=10_000.0,
        daily_realized_pnl=0.0,
        ts_ms=123,
    )
    defaults.update(kw)
    return PortfolioState(**defaults)


class TestPortfolioState:
    def test_open_count(self):
        p = _portfolio(open_positions=(_pos(), _pos(symbol="ETHUSDT")))
        assert p.open_count == 2

    def test_drawdown_zero_at_peak(self):
        p = _portfolio(equity=10_000, peak_equity=10_000)
        assert p.drawdown_pct == pytest.approx(0.0)

    def test_drawdown_positive_when_below_peak(self):
        p = _portfolio(equity=9_000, peak_equity=10_000)
        assert p.drawdown_pct == pytest.approx(0.10)

    def test_drawdown_never_negative(self):
        p = _portfolio(equity=12_000, peak_equity=10_000)
        assert p.drawdown_pct == pytest.approx(0.0)

    def test_total_risk_fraction(self):
        p = _portfolio(equity=10_000, open_positions=(_pos(risk=100.0),))
        assert p.total_risk_pct == pytest.approx(0.01)

    def test_total_risk_sums_all_positions(self):
        p = _portfolio(
            equity=10_000,
            open_positions=(_pos(risk=100.0), _pos(risk=100.0)),
        )
        assert p.total_risk_pct == pytest.approx(0.02)

    def test_group_exposures_correlated(self):
        p = _portfolio(
            equity=10_000,
            open_positions=(_pos(symbol="BTCUSDT", risk=100.0),
                            _pos(symbol="ETHUSDT", risk=100.0)),
        )
        exp = p.group_exposures((("BTC", "ETH"),))
        assert exp["BTC_ETH"] == pytest.approx(0.02)

    def test_group_exposures_uncorrelated_kept_separate(self):
        p = _portfolio(
            equity=10_000,
            open_positions=(_pos(symbol="BTCUSDT", risk=100.0),
                            _pos(symbol="SOLUSDT", risk=100.0)),
        )
        exp = p.group_exposures((("BTC", "ETH"),))
        assert exp["BTC_ETH"] == pytest.approx(0.01)
        assert exp["SOL"] == pytest.approx(0.01)

    def test_symbols_open(self):
        p = _portfolio(open_positions=(_pos(), _pos(symbol="ETHUSDT")))
        assert p.symbols_open == frozenset({"BTCUSDT", "ETHUSDT"})

    def test_regime_exposures(self):
        p = _portfolio(
            equity=10_000,
            open_positions=(_pos(risk=50.0, regime="range"),
                            _pos(symbol="ETHUSDT", risk=50.0, regime="range")),
        )
        assert p.regime_exposures["range"] == pytest.approx(0.01)

    def test_immutable_field_assignment_raises(self):
        p = _portfolio()
        with pytest.raises(Exception):
            p.equity = 9999  # frozen dataclass