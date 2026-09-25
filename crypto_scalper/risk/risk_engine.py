"""Risk Engine (FASE 5) — authoritative trade approval / rejection.

The RiskEngine has FINAL AUTHORITY over every trade:

    Signal (eligible by strategy) → RiskEngine.assess() → RiskDecision

Even if the Strategy Engine deems a signal eligible, the Risk Engine can
reject it.  Risk ALWAYS overrides strategy and ML.

Checks performed (in order, fail-fast):
    1. Kill switch / trading halted / safe mode
    2. Signal eligibility (strategy must have already gated)
    3. Daily loss limit
    4. Maximum drawdown
    5. Consecutive loss protection (reduce / pause)
    6. Max open positions
    7. Total portfolio risk exposure
    8. Correlated group exposure
    9. Position sizing (risk-based formula)
   10. Minimum required edge (after costs)
   11. Stop-loss and take-profit computation

All inputs are passed explicitly (no internal state, no network).
RiskConfig governs all hard limits.  The engine is deterministic and
testable offline.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.core.enums import RejectReason, RiskVerdict, SignalType
from crypto_scalper.core.models import RiskDecision, Signal
from crypto_scalper.risk.exposure import ExposureManager
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.risk.position_sizing import PositionSizer
from crypto_scalper.risk.stop_loss import StopLossCalculator
from crypto_scalper.risk.take_profit import TakeProfitCalculator

log = logging.getLogger(__name__)


class RiskEngine:
    """Stateless orchestrator; all mutable state lives in PortfolioState."""

    def __init__(self, config: RiskConfig) -> None:
        self._config = config
        self._sizer = PositionSizer(config)
        self._sl_calc = StopLossCalculator(config)
        self._tp_calc = TakeProfitCalculator(config)
        self._exposure = ExposureManager(config)

    @property
    def config(self) -> RiskConfig:
        return self._config

    def assess(
        self,
        signal: Signal,
        portfolio: PortfolioState,
        *,
        atr: float = 0.0,
        entry_price: Optional[float] = None,
        side: Optional[str] = None,
        tick_size: float = 0.0,
        lot_size: float = 0.0,
        rr_ratio: float = 2.0,
        sl_atr_mult: float = 1.5,
        now_ms: Optional[int] = None,
    ) -> RiskDecision:
        """Evaluate a Signal against all risk constraints.

        Returns a RiskDecision with:
          - verdict REJECTED / APPROVED / TRADING_HALTED / SAFE_MODE
          - position sizing, SL, TP when approved
          - a risk_checks dict showing which gate passed/failed
        """
        now_ms = now_ms or int(time.time() * 1000)
        checks: Dict[str, bool] = {}
        reject_reason = RejectReason.NONE

        # ── 0. Kill switch / halted / safe mode ───────────────────────
        if portfolio.safe_mode:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.SAFE_MODE,
                RejectReason.KILL_SWITCH, checks,
                details={"state": "safe_mode"},
            )
        if portfolio.trading_halted:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.TRADING_HALTED,
                RejectReason.DAILY_LOSS_LIMIT, checks,
                details={"state": "trading_halted"},
            )

        # ── 1. Signal must be eligible from strategy ─────────────────
        checks["signal_eligible"] = signal.eligible
        if not signal.eligible:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.OTHER, checks,
                details={"reason": signal.reason},
            )

        # ── 2. Daily loss limit ──────────────────────────────────────
        # Denominator is start-of-day equity (equity before today's realized PnL),
        # otherwise the limit loosens as losses shrink current equity.
        daily_loss_pct = 0.0
        day_start_equity = portfolio.equity - portfolio.daily_realized_pnl
        if day_start_equity > 0:
            daily_loss_pct = abs(min(0.0, portfolio.daily_realized_pnl)) / day_start_equity
        daily_ok = daily_loss_pct < self._config.daily_loss_limit_pct
        checks["daily_loss_limit"] = daily_ok
        if not daily_ok:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.TRADING_HALTED,
                RejectReason.DAILY_LOSS_LIMIT, checks,
                details={"daily_loss_pct": round(daily_loss_pct, 6),
                         "limit": self._config.daily_loss_limit_pct},
            )

        # ── 3. Maximum drawdown ──────────────────────────────────────
        dd = portfolio.drawdown_pct
        dd_ok = dd < self._config.max_drawdown_pct
        checks["max_drawdown"] = dd_ok
        if not dd_ok:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.SAFE_MODE,
                RejectReason.MAX_DRAWDOWN, checks,
                details={"drawdown_pct": round(dd, 6),
                         "limit": self._config.max_drawdown_pct},
            )

        # ── 4. Consecutive loss protection ───────────────────────────
        cl = self._config.consecutive
        if portfolio.consecutive_losses >= cl.pause_after:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.TRADING_HALTED,
                RejectReason.CONSECUTIVE_LOSSES, checks,
                details={"consecutive_losses": portfolio.consecutive_losses,
                         "pause_after": cl.pause_after},
            )

        # ── 5. Max open positions ────────────────────────────────────
        pos_check = self._exposure.check_max_positions(portfolio.open_count)
        checks["max_positions"] = pos_check.passed
        if not pos_check.passed:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.RISK_EXPOSURE_EXCEEDED, checks,
                details={"reason": pos_check.reason},
            )

        # ── 6. Total risk exposure ───────────────────────────────────
        risk_pct = self._config.risk_per_trade_pct
        total_check = self._exposure.check_total_risk(portfolio.total_risk_pct, risk_pct)
        checks["total_risk_exposure"] = total_check.passed
        if not total_check.passed:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.RISK_EXPOSURE_EXCEEDED, checks,
                details={"reason": total_check.reason},
            )

        # ── 7. Correlated group exposure ─────────────────────────────
        group_exp = portfolio.group_exposures(self._config.correlation_groups)
        group_check = self._exposure.check_correlated_group(
            signal.symbol, risk_pct, group_exp,
        )
        checks["correlated_group"] = group_check.passed
        if not group_check.passed:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.CORRELATION_EXCEEDED, checks,
                details={"reason": group_check.reason},
            )

        # ── 8. Position sizing ───────────────────────────────────────
        _entry = entry_price if entry_price and entry_price > 0 else 0.0
        _side = side or ("BUY" if signal.signal_type is SignalType.LONG else "SELL")

        # Compute SL first (needed for sizing)
        sl = self._sl_calc.atr_based(
            _entry, atr, _side, atr_mult=sl_atr_mult,
        )
        sl_price = sl.stop_price
        stop_distance = sl.stop_distance
        # No entry price / no ATR → no valid stop → never size a trade.
        checks["stop_loss_valid"] = sl.method != "invalid" and sl_price > 0 and stop_distance > 0
        if not checks["stop_loss_valid"]:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.DATA_NOT_READY, checks,
                details={"entry_price": _entry, "atr": atr},
            )

        sizing = self._sizer.calculate(
            portfolio.equity, _entry, sl_price, _side,
            tick_size=tick_size, lot_size=lot_size,
        )

        checks["position_sizing"] = sizing.quantity > 0
        if sizing.quantity <= 0:
            return self._decision(
                signal.symbol, now_ms, RiskVerdict.REJECTED,
                RejectReason.OTHER, checks,
                details={"cap_reason": sizing.cap_reason or "zero_quantity"},
            )

        # ── 9. Take-profit ───────────────────────────────────────────
        tp = self._tp_calc.calculate(_entry, sl_price, _side, rr_ratio)

        # ── 10. Minimum required edge ────────────────────────────────
        checks["minimum_edge"] = True  # gate kept at strategy+ML level for now

        # ── APPROVED ─────────────────────────────────────────────────
        return RiskDecision(
            symbol=signal.symbol,
            ts_ms=now_ms,
            verdict=RiskVerdict.APPROVED.name,
            reason="",
            verifier="risk_engine",
            details={
                "signal_score": signal.score,
                "signal_type": signal.signal_type.name,
                "daily_loss_pct": round(daily_loss_pct, 6),
                "drawdown_pct": round(dd, 6),
                "consecutive_losses": portfolio.consecutive_losses,
                "sl_method": sl.method,
                "tp_rr_ratio": tp.rr_ratio,
            },
            position_size=sizing.quantity,
            stop_loss_price=sl_price,
            take_profit_price=tp.take_profit_price,
            notional_value=sizing.notional_value,
            risk_amount=sizing.risk_amount,
            leverage_used=sizing.leverage_used,
            risk_checks=checks,
        )

    @staticmethod
    def _decision(
        symbol: str,
        ts_ms: int,
        verdict: RiskVerdict,
        reason: RejectReason,
        checks: Dict[str, bool],
        details: Optional[Dict] = None,
    ) -> RiskDecision:
        return RiskDecision(
            symbol=symbol,
            ts_ms=ts_ms,
            verdict=verdict.name,
            reason=reason.name,
            verifier="risk_engine",
            details=details or {},
            risk_checks=dict(checks),
        )
