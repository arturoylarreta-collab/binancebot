"""Exposure management (FASE 5).

Tracks and enforces:
    - max risk per trade
    - max total open risk across all positions
    - max number of simultaneous positions
    - max exposure per correlated asset group
    - max exposure per market regime

All limits come from RiskConfig; the manager is stateless per call
(all state lives in the PortfolioState passed in).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from crypto_scalper.config.risk import RiskConfig


@dataclass(frozen=True)
class ExposureCheck:
    passed: bool
    reason: str = ""


class ExposureManager:
    def __init__(self, config: RiskConfig) -> None:
        self._config = config

    def check_max_positions(
        self,
        current_open_count: int,
    ) -> ExposureCheck:
        if current_open_count >= self._config.max_positions:
            return ExposureCheck(
                passed=False,
                reason=f"max_positions={self._config.max_positions} reached "
                       f"(open={current_open_count})",
            )
        return ExposureCheck(passed=True)

    def check_total_risk(
        self,
        current_total_risk_pct: float,
        proposed_risk_pct: float,
    ) -> ExposureCheck:
        combined = current_total_risk_pct + proposed_risk_pct
        cap = self._config.max_total_open_risk_pct
        if combined >= cap:
            return ExposureCheck(
                passed=False,
                reason=f"total_risk={combined:.4f} would exceed cap {cap:.4f}",
            )
        return ExposureCheck(passed=True)

    def check_correlated_group(
        self,
        symbol: str,
        proposed_risk_pct: float,
        group_exposures: Dict[str, float],
    ) -> ExposureCheck:
        group = self._find_group(symbol)
        if group is None:
            return ExposureCheck(passed=True)

        key = "_".join(sorted(group))
        current = group_exposures.get(key, 0.0)
        combined = current + proposed_risk_pct
        cap = self._config.correlated_group_exposure_cap_pct
        if combined >= cap:
            return ExposureCheck(
                passed=False,
                reason=f"correlated group {group} exposure={combined:.4f} "
                       f"would exceed cap {cap:.4f}",
            )
        return ExposureCheck(passed=True)

    def _find_group(self, symbol: str) -> Optional[Tuple[str, ...]]:
        base = symbol.upper().replace("USDT", "").replace("BUSD", "")
        for group in self._config.correlation_groups:
            if base in group:
                return group
        return None
