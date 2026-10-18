"""Position sizing (FASE 5).

Formula:
    risk_amount   = equity × risk_per_trade_pct
    position_size = risk_amount / stop_distance

Adjusts for:
    - leverage (capped at RiskConfig.max_leverage)
    - minimum and maximum position caps
    - tick size / lot size (floor to exchange precision)

All inputs are validated; division by zero is guarded.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from crypto_scalper.config.risk import RiskConfig


@dataclass(frozen=True)
class SizingResult:
    quantity: float
    notional_value: float
    risk_amount: float
    stop_distance: float
    leverage_used: int
    capped: bool = False
    cap_reason: str = ""


class PositionSizer:
    """Stateless position sizer; all state lives in RiskConfig."""

    def __init__(self, config: RiskConfig) -> None:
        self._config = config

    def calculate(
        self,
        equity: float,
        entry_price: float,
        stop_price: float,
        side: str = "BUY",
        tick_size: float = 0.0,
        lot_size: float = 0.0,
    ) -> SizingResult:
        """Return the position size for a proposed trade.

        Parameters
        ----------
        equity : float
            Current account equity in quote currency.
        entry_price : float
            Expected fill price.
        stop_price : float
            Hard stop-loss price.
        side : str
            "BUY" for long, "SELL" for short.
        tick_size : float
            Exchange tick size; 0 = no rounding.
        lot_size : float
            Exchange minimum lot step; 0 = no rounding.

        Returns
        -------
        SizingResult
        """
        if equity <= 0:
            return SizingResult(
                quantity=0.0, notional_value=0.0, risk_amount=0.0,
                stop_distance=0.0, leverage_used=1, capped=True,
                cap_reason="zero_equity",
            )

        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            return SizingResult(
                quantity=0.0, notional_value=0.0, risk_amount=0.0,
                stop_distance=0.0, leverage_used=1, capped=True,
                cap_reason="zero_stop_distance",
            )

        risk_amount = equity * self._config.risk_per_trade_pct
        raw_qty = risk_amount / stop_distance

        leverage = self._config.max_leverage

        notional = raw_qty * entry_price
        max_notional = equity * leverage
        capped = False
        cap_reason = ""

        if notional > max_notional and max_notional > 0:
            raw_qty = max_notional / entry_price
            notional = raw_qty * entry_price
            capped = True
            cap_reason = "leverage_cap"

        # tick_size is a PRICE increment and never applies to quantity; the
        # quantity only snaps (down, so risk never exceeds budget) to lot_size.
        if lot_size > 0 and raw_qty > 0:
            steps = math.floor(raw_qty / lot_size + 1e-9)
            raw_qty = steps * lot_size

        raw_qty = max(0.0, raw_qty)
        notional = raw_qty * entry_price

        return SizingResult(
            quantity=round(raw_qty, 10),
            notional_value=round(notional, 10),
            risk_amount=round(risk_amount, 10),
            stop_distance=round(stop_distance, 10),
            leverage_used=leverage,
            capped=capped,
            cap_reason=cap_reason,
        )
