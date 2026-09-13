"""Stop-loss calculation (FASE 5).

Methods:
    ATR-based (primary):  entry ± atr_mult × ATR
    Fixed percentage:     entry × (1 ∓ pct)

Clamped to:
    min_stop_distance  (avoids stops too tight)
    max_stop_distance  (avoids stops too wide)

All calculations are offline/deterministic; no market data fetching.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from crypto_scalper.config.risk import RiskConfig


@dataclass(frozen=True)
class StopLossResult:
    stop_price: float
    stop_distance: float
    method: str


class StopLossCalculator:
    def __init__(self, config: RiskConfig) -> None:
        self._config = config

    def atr_based(
        self,
        entry_price: float,
        atr: float,
        side: str = "BUY",
        atr_mult: float = 1.5,
    ) -> StopLossResult:
        if entry_price <= 0 or atr <= 0:
            return StopLossResult(stop_price=0.0, stop_distance=0.0, method="invalid")

        raw_distance = atr_mult * atr
        distance = self._clamp_distance(raw_distance, entry_price)

        if side.upper() == "BUY":
            stop_price = entry_price - distance
        else:
            stop_price = entry_price + distance

        return StopLossResult(
            stop_price=round(stop_price, 10),
            stop_distance=round(distance, 10),
            method="atr",
        )

    def fixed_pct(
        self,
        entry_price: float,
        side: str = "BUY",
        pct: float = 0.005,
    ) -> StopLossResult:
        if entry_price <= 0 or pct <= 0:
            return StopLossResult(stop_price=0.0, stop_distance=0.0, method="invalid")

        raw_distance = entry_price * pct
        distance = self._clamp_distance(raw_distance, entry_price)

        if side.upper() == "BUY":
            stop_price = entry_price - distance
        else:
            stop_price = entry_price + distance

        return StopLossResult(
            stop_price=round(stop_price, 10),
            stop_distance=round(distance, 10),
            method="fixed_pct",
        )

    def _clamp_distance(self, raw: float, reference_price: float) -> float:
        min_dist = reference_price * 0.0005
        max_dist = reference_price * 0.05
        return max(min_dist, min(max_dist, raw))
