"""Backtest configuration (FASE 8).

Controls the offline backtesting stack: where historical klines live, the
bar interval, the execution policy (fill at bar close vs next open), the
feature warm-up, the optional expected-edge gate and the explicit cost model
(fees, spread, funding, latency + slippage).

Everything has a safe, explicit default and is validated here. Nothing in
this module is a secret.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from crypto_scalper.core.exceptions import ConfigurationError

_FILL_AT_CHOICES = ("close", "next_open")


@dataclass(frozen=True)
class CostModelConfig:
    """Explicit per-side cost assumptions for FASE 8.

    `slippage_pct` is BOTH embedded in fill prices by the bar venue (market
    impact + execution delay, so gross PnL already reflects it) and part of
    the expected-edge gate. `fee/spread/latency/funding` are additional,
    reported as attribution on top of gross PnL.
    """

    fee_pct: float = 0.0004               # taker fee per side (entry + exit)
    spread_pct: float = 0.0001            # structural spread effect per side
    slippage_pct: float = 0.0005          # impact/delay, embedded in fills
    latency_pct: float = 0.0001           # latency buffer per side
    funding_pct_per_8h: float = 0.0001    # futures funding financing per 8h
    minimum_required_edge_pct: float = 0.0015

    def __post_init__(self) -> None:
        for name, value in (
            ("fee_pct", self.fee_pct),
            ("spread_pct", self.spread_pct),
            ("slippage_pct", self.slippage_pct),
            ("latency_pct", self.latency_pct),
            ("funding_pct_per_8h", self.funding_pct_per_8h),
            ("minimum_required_edge_pct", self.minimum_required_edge_pct),
        ):
            if not (0.0 <= value < 0.10):
                raise ConfigurationError(
                    f"{name} must be in [0, 0.10), got {value}"
                )


@dataclass(frozen=True)
class BacktestConfig:
    data_dir: Path = Path("data/klines")      # CSV store for historical klines
    interval_s: int = 60                      # kline interval in seconds (60 = 1m)
    warmup_bars: int = 60                     # skip decisions until this many bars exist
    fill_at: str = "close"                    # "close" | "next_open" (execution policy)
    start_equity: float = 10_000.0
    max_open_per_symbol: int = 1
    entry_fill_fraction: float = 1.0          # 0 < f <= 1: deterministic partial entry fills
    enforce_edge_gate: bool = False           # block trades failing expected-edge-after-costs
    costs: CostModelConfig = field(default_factory=CostModelConfig)
    db_path: str = ""                         # optional sqlite audit trail ("" = noop)
    model_path: Path = Path("models/ml-predictor.joblib")  # optional ML filter

    def __post_init__(self) -> None:
        if self.interval_s not in (1, 60, 180, 300, 900, 1800, 3600, 14400, 86400):
            raise ConfigurationError(
                f"interval_s must be a supported kline interval, got {self.interval_s}"
            )
        if self.warmup_bars < 2:
            raise ConfigurationError(f"warmup_bars must be >= 2, got {self.warmup_bars}")
        if self.fill_at not in _FILL_AT_CHOICES:
            raise ConfigurationError(
                f"fill_at must be one of {_FILL_AT_CHOICES}, got {self.fill_at!r}"
            )
        if self.start_equity <= 0:
            raise ConfigurationError(f"start_equity must be > 0, got {self.start_equity}")
        if self.max_open_per_symbol < 1:
            raise ConfigurationError(
                f"max_open_per_symbol must be >= 1, got {self.max_open_per_symbol}"
            )
        if not (0.0 < self.entry_fill_fraction <= 1.0):
            raise ConfigurationError(
                f"entry_fill_fraction must be in (0, 1], got {self.entry_fill_fraction}"
            )