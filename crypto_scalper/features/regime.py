"""MARKET REGIME classifier.

Distinguishes trending / ranging / volatile / breakout states so strategies
(FASE 3) can gate activity per regime. Never forced: UNKNOWN is a valid
output (normally means: do nothing).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from crypto_scalper.core.enums import Regime


@dataclass(frozen=True)
class RegimeThresholds:
    adx_trend: float = 25.0
    adx_range: float = 20.0
    atr_pct_low: float = 0.0008
    atr_pct_high: float = 0.0030
    atr_pct_extreme: float = 0.0100


class MarketRegime:
    def __init__(self, thresholds: RegimeThresholds = RegimeThresholds()) -> None:
        self._t = thresholds

    def classify(
        self,
        price: float,
        ema9: float,
        ema21: float,
        ema50: float,
        adx: float,
        atr_pct: float,
        boll_upper: float,
        boll_lower: float,
    ) -> Regime:
        if atr_pct >= self._t.atr_pct_extreme:
            return Regime.EXTREME

        if boll_upper and price is not None and float(price) > boll_upper:
            return Regime.BREAKOUT
        if boll_lower and price is not None and float(price) < boll_lower:
            return Regime.BREAKOUT

        if adx >= self._t.adx_trend:
            if ema9 > ema21 > ema50:
                return Regime.TRENDING_UP
            if ema9 < ema21 < ema50:
                return Regime.TRENDING_DOWN
            # strong ADX without a clean stack: follow the fast-vs-slow EMA sign
            return Regime.TRENDING_UP if ema9 >= ema50 else Regime.TRENDING_DOWN

        if adx < self._t.adx_range:
            if atr_pct >= self._t.atr_pct_high:
                return Regime.HIGH_VOLATILITY
            if atr_pct <= self._t.atr_pct_low:
                return Regime.LOW_VOLATILITY
            return Regime.RANGE

        if atr_pct >= self._t.atr_pct_high:
            return Regime.HIGH_VOLATILITY
        return Regime.UNKNOWN

    @staticmethod
    def prefer_deploying(regime: Regime) -> bool:
        """Strategies (FASE 3) may use this to gate signal generation."""
        return regime in (
            Regime.TRENDING_UP,
            Regime.TRENDING_DOWN,
            Regime.BREAKOUT,
            Regime.RANGE,
        )