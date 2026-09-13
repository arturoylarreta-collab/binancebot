"""Take-profit calculation (FASE 5).

TP is derived from the risk:reward ratio applied to the stop distance.

    stop_distance = |entry - stop|
    tp_distance   = rr_ratio × stop_distance
    tp_price      = entry ± tp_distance

Minimum reward:risk is enforced via RiskConfig.min_cost_edge_ratio.
"""

from __future__ import annotations

from dataclasses import dataclass

from crypto_scalper.config.risk import RiskConfig


@dataclass(frozen=True)
class TakeProfitResult:
    take_profit_price: float
    tp_distance: float
    rr_ratio: float


class TakeProfitCalculator:
    def __init__(self, config: RiskConfig) -> None:
        self._config = config

    def calculate(
        self,
        entry_price: float,
        stop_price: float,
        side: str = "BUY",
        rr_ratio: float = 2.0,
    ) -> TakeProfitResult:
        stop_distance = abs(entry_price - stop_price)

        rr = max(rr_ratio, self._config.min_cost_edge_ratio)

        tp_distance = stop_distance * rr

        if side.upper() == "BUY":
            tp_price = entry_price + tp_distance
        else:
            tp_price = entry_price - tp_distance

        return TakeProfitResult(
            take_profit_price=round(tp_price, 10),
            tp_distance=round(tp_distance, 10),
            rr_ratio=round(rr, 4),
        )
