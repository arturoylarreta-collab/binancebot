"""Risk configuration.

Defined and validated in FASE 1; consumed by the Risk Engine in FASE 5.
Everything here is a hard limit; nothing is permissively large by default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple

from crypto_scalper.core.exceptions import ConfigurationError

# Hard ceiling regardless of configuration
ABSOLUTE_MAX_RISK_PER_TRADE_PCT = 0.01
ABSOLUTE_MAX_LEVERAGE = 3


@dataclass(frozen=True)
class ConsecutiveLossProtection:
    reduce_risk_after: int = 3
    pause_after: int = 4
    risk_reduction_factor: float = 0.5


@dataclass(frozen=True)
class RiskConfig:
    # Per-trade risk as a fraction of equity (0.005 = 0.5%)
    risk_per_trade_pct: float = 0.01
    max_total_open_risk_pct: float = 0.03
    max_positions: int = 10
    max_leverage: int = 1
    daily_loss_limit_pct: float = 0.03
    max_drawdown_pct: float = 0.10
    stop_loss_enabled: bool = True
    take_profit_enabled: bool = True
    correlated_group_exposure_cap_pct: float = 0.06
    max_correlation_for_group: float = 0.70
    correlation_groups: Tuple[Tuple[str, ...], ...] = (("BTC", "ETH", "SOL"),)
    consecutive: ConsecutiveLossProtection = field(
        default_factory=ConsecutiveLossProtection
    )
    minimum_required_edge_pct: float = 0.0015  # after costs, FASE 5 uses this
    min_cost_edge_ratio: float = 1.5

    def __post_init__(self) -> None:
        if not (0 < self.risk_per_trade_pct <= ABSOLUTE_MAX_RISK_PER_TRADE_PCT):
            raise ConfigurationError(
                f"risk_per_trade_pct={self.risk_per_trade_pct} > hard cap "
                f"{ABSOLUTE_MAX_RISK_PER_TRADE_PCT}"
            )
        if self.max_leverage > ABSOLUTE_MAX_LEVERAGE:
            raise ConfigurationError(
                f"max_leverage={self.max_leverage} > hard cap {ABSOLUTE_MAX_LEVERAGE}; "
                "start at 1x-3x only"
            )
        if self.daily_loss_limit_pct <= 0 or self.daily_loss_limit_pct > 0.10:
            raise ConfigurationError("daily_loss_limit_pct must be in (0, 0.10]")
        if self.max_drawdown_pct <= 0 or self.max_drawdown_pct > 0.25:
            raise ConfigurationError("max_drawdown_pct must be in (0, 0.25]")
        if self.max_positions < 1:
            raise ConfigurationError("max_positions must be >= 1")


@dataclass(frozen=True)
class EnvironmentLimits:
    """Explicit guardrails per environment; LIVE requires them all."""
    allow_trading: bool
    max_gross_notional_usd: float = 0.0
    allowed_symbols: List[str] = field(default_factory=list)

    @classmethod
    def for_env(cls, env_name: str, settings_risk: RiskConfig) -> "EnvironmentLimits":
        env = env_name.lower()
        if env == "dev":
            return cls(allow_trading=False)
        if env == "paper":
            return cls(allow_trading=True, max_gross_notional_usd=1_000_000.0)
        if env == "live":
            return cls(
                allow_trading=True,
                max_gross_notional_usd=50_000.0,
            )
        raise ConfigurationError(f"unknown environment {env_name!r}")