"""Portfolio state for offline risk assessment (FASE 5).

A pure data structure that captures the current account snapshot:
equity, open positions, realized/unrealized PnL, drawdown, and
consecutive losses.  No network, no persistence, no side effects.

RiskEngine consumes a PortfolioState on every assess() call.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional


@dataclass(frozen=True)
class Position:
    symbol: str
    side: str               # "BUY" or "SELL"
    entry_price: float
    quantity: float
    stop_loss_price: float
    take_profit_price: float
    notional_value: float
    risk_amount: float
    opened_ts_ms: int = 0
    regime: str = "unknown"

    @property
    def base(self) -> str:
        """Base symbol without quote suffix."""
        return self.symbol.upper().replace("USDT", "").replace("BUSD", "")


@dataclass(frozen=True)
class PortfolioState:
    equity: float
    initial_equity: float
    open_positions: tuple = ()
    daily_realized_pnl: float = 0.0
    peak_equity: float = 0.0
    consecutive_losses: int = 0
    trading_halted: bool = False
    safe_mode: bool = False
    ts_ms: int = 0

    def __post_init__(self) -> None:
        if self.ts_ms == 0:
            object.__setattr__(self, "ts_ms", int(time.time() * 1000))

    @property
    def open_count(self) -> int:
        return len(self.open_positions)

    @property
    def total_risk_pct(self) -> float:
        if self.equity <= 0:
            return 0.0
        total = sum(p.risk_amount for p in self.open_positions)
        return total / self.equity

    @property
    def unrealized_pnl(self) -> float:
        return 0.0

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - self.equity) / self.peak_equity)

    def group_exposures(self, correlation_groups: tuple = ()) -> Dict[str, float]:
        """Risk fraction per correlation group (base symbols only)."""
        if self.equity <= 0:
            return {}
        result: Dict[str, float] = {}
        for pos in self.open_positions:
            base = pos.base
            assigned = False
            for group in correlation_groups:
                if base in group:
                    key = "_".join(sorted(group))
                    result[key] = result.get(key, 0.0) + pos.risk_amount / self.equity
                    assigned = True
                    break
            if not assigned:
                result[base] = result.get(base, 0.0) + pos.risk_amount / self.equity
        return result

    @property
    def regime_exposures(self) -> Dict[str, float]:
        if self.equity <= 0:
            return {}
        result: Dict[str, float] = {}
        for pos in self.open_positions:
            result[pos.regime] = result.get(pos.regime, 0.0) + pos.risk_amount / self.equity
        return result

    @property
    def symbols_open(self) -> FrozenSet[str]:
        return frozenset(p.symbol for p in self.open_positions)
