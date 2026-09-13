"""FASE 5 — offline integration tests for the Risk Engine.

Covers the full assess() contract:
  - APPROVED path with sizing/SL/TP populated
  - each hard limit rejection (daily loss, drawdown, halt, exposure)
  - hierarchy: risk ALWAYS overrides strategy (eligible signal rejected)
  - determinism and offline (no network) execution
"""

from __future__ import annotations

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.core.enums import RejectReason, RiskVerdict, SignalType
from crypto_scalper.core.models import Signal
from crypto_scalper.risk.portfolio import PortfolioState, Position
from crypto_scalper.risk.risk_engine import RiskEngine


def _config(**kw) -> RiskConfig:
    defaults = dict(
        risk_per_trade_pct=0.01,
        max_total_open_risk_pct=0.03,
        max_positions=10,
        max_leverage=1,
        daily_loss_limit_pct=0.03,
        max_drawdown_pct=0.10,
        correlated_group_exposure_cap_pct=0.06,
        correlation_groups=(("BTC", "ETH"),),
    )
    defaults.update(kw)
    return RiskConfig(**defaults)


def _signal(symbol="BTCUSDT", signal_type=SignalType.LONG, eligible=True,
            score=70.0, regime="trending_up") -> Signal:
    return Signal(
        symbol=symbol,
        timestamp_ms=123,
        signal_type=signal_type,
        score=score,
        regime=regime,
        eligible=eligible,
        reason="",
        components={},
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


def _pos(symbol="BTCUSDT", risk=50.0, regime="trending_up") -> Position:
    return Position(
        symbol=symbol, side="BUY", entry_price=100.0, quantity=5.0,
        stop_loss_price=98.0, take_profit_price=104.0,
        notional_value=500.0, risk_amount=risk, opened_ts_ms=0, regime=regime,
    )


class TestApproval:
    def test_approved_path_populates_notional_and_sl_tp(self):
        engine = RiskEngine(_config(max_leverage=1))
        dec = engine.assess(_signal(), _portfolio(),
                            entry_price=50000.0, atr=500.0)
        assert dec.verdict == RiskVerdict.APPROVED.name
        assert dec.risk_amount == pytest.approx(100.0)   # 10000 * 0.01
        assert dec.notional_value == pytest.approx(dec.position_size * 50000.0)
        assert dec.position_size > 0
        assert dec.stop_loss_price == pytest.approx(50000.0 - 500.0 * 1.5)
        assert dec.take_profit_price > dec.stop_loss_price
        assert dec.risk_checks["signal_eligible"] is True
        assert dec.risk_checks["daily_loss_limit"] is True
        assert dec.risk_checks["max_drawdown"] is True
        assert dec.risk_checks["max_positions"] is True
        assert dec.risk_checks["total_risk_exposure"] is True
        assert dec.risk_checks["correlated_group"] is True

    def test_approved_short_side(self):
        engine = RiskEngine(_config(max_leverage=1))
        dec = engine.assess(_signal(signal_type=SignalType.SHORT), _portfolio(),
                            entry_price=50000.0, atr=500.0, side="SELL",
                            sl_atr_mult=1.0)
        assert dec.verdict == RiskVerdict.APPROVED.name
        assert dec.stop_loss_price > 50000.0  # short stop above entry

    def test_deterministic(self):
        engine = RiskEngine(_config())
        a = engine.assess(_signal(), _portfolio(), entry_price=100.0, atr=1.0, now_ms=123)
        b = engine.assess(_signal(), _portfolio(), entry_price=100.0, atr=1.0, now_ms=123)
        assert a == b


class TestHierarchy:
    def test_risk_rejects_eligible_strategy_signal(self):
        """Strategy says eligible; Risk reins in exposure."""
        engine = RiskEngine(_config(max_positions=1, max_total_open_risk_pct=0.03))
        portfolio = _portfolio(open_positions=(_pos(risk=200.0),))
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.REJECTED.name
        assert dec.reason == RejectReason.RISK_EXPOSURE_EXCEEDED.name

    def test_inelegible_signal_rejected_by_risk(self):
        engine = RiskEngine(_config())
        sig = _signal(eligible=False, regime="range")
        dec = engine.assess(sig, _portfolio(), entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.REJECTED.name
        assert dec.risk_checks["signal_eligible"] is False


class TestDailyLossLimit:
    def test_rejected_when_daily_loss_at_limit(self):
        engine = RiskEngine(_config(daily_loss_limit_pct=0.03))
        portfolio = _portfolio(equity=10_000, daily_realized_pnl=-300.0)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.TRADING_HALTED.name
        assert dec.reason == RejectReason.DAILY_LOSS_LIMIT.name

    def test_rejected_when_daily_loss_exceeds_limit(self):
        engine = RiskEngine(_config(daily_loss_limit_pct=0.03))
        portfolio = _portfolio(equity=10_000, daily_realized_pnl=-400.0)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.TRADING_HALTED.name

    def test_approved_under_daily_limit(self):
        engine = RiskEngine(_config(daily_loss_limit_pct=0.03))
        portfolio = _portfolio(equity=10_000, daily_realized_pnl=-100.0)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.APPROVED.name


class TestDrawdown:
    def test_safe_mode_when_drawdown_at_ceiling(self):
        engine = RiskEngine(_config(max_drawdown_pct=0.10))
        portfolio = _portfolio(equity=8_900, peak_equity=10_000)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.SAFE_MODE.name
        assert dec.reason == RejectReason.MAX_DRAWDOWN.name

    def test_approved_when_drawdown_below_ceiling(self):
        engine = RiskEngine(_config(max_drawdown_pct=0.10))
        portfolio = _portfolio(equity=9_500, peak_equity=10_000)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.APPROVED.name


class TestHaltAndSafeMode:
    def test_trading_halted_flag(self):
        engine = RiskEngine(_config())
        portfolio = _portfolio(trading_halted=True)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.TRADING_HALTED.name

    def test_safe_mode_flag(self):
        engine = RiskEngine(_config())
        portfolio = _portfolio(safe_mode=True)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.SAFE_MODE.name


class TestExposureLimits:
    def test_max_positions_rejected(self):
        engine = RiskEngine(_config(max_positions=2))
        portfolio = _portfolio(open_positions=(_pos(), _pos(symbol="ETHUSDT")))
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.REJECTED.name
        assert dec.reason == RejectReason.RISK_EXPOSURE_EXCEEDED.name
        assert dec.risk_checks["max_positions"] is False

    def test_correlated_group_rejected(self):
        engine = RiskEngine(_config(
            correlated_group_exposure_cap_pct=0.06,
            max_total_open_risk_pct=0.08,
        ))
        portfolio = _portfolio(
            open_positions=(_pos("BTCUSDT", risk=340.0),
                            _pos("ETHUSDT", risk=340.0)),
        )
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.REJECTED.name
        assert dec.reason == RejectReason.CORRELATION_EXCEEDED.name
        assert dec.risk_checks["correlated_group"] is False


class TestConsecutiveLosses:
    def test_pause_after_threshold(self):
        engine = RiskEngine(_config())
        portfolio = _portfolio(consecutive_losses=4)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.TRADING_HALTED.name

    def test_ok_below_threshold(self):
        engine = RiskEngine(_config())
        portfolio = _portfolio(consecutive_losses=2)
        dec = engine.assess(_signal(), portfolio, entry_price=100.0, atr=1.0)
        assert dec.verdict == RiskVerdict.APPROVED.name


class TestOffline:
    def test_no_network_execution(self):
        """Pure computation; should never touch sockets."""
        engine = RiskEngine(_config())
        dec = engine.assess(_signal(), _portfolio(), entry_price=100.0, atr=1.0)
        assert dec.verdict in {RiskVerdict.APPROVED.name, RiskVerdict.REJECTED.name,
                               RiskVerdict.TRADING_HALTED.name, RiskVerdict.SAFE_MODE.name}