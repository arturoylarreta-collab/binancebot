"""Strategy configuration.

FASE 3 defines signal-scoring weights, regime gating and direction
thresholds here. Weights are NOT claimed optimal; they are a starting point
to be validated by backtesting (FASE 8). Keep shapes fixed and validated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

from crypto_scalper.core.enums import Regime
from crypto_scalper.core.exceptions import ConfigurationError

# Default Signal Score components (FASE 3). Every value is a dimension of
# the final 0-100 Signal Score; they sum to 1.0.
DEFAULT_SIGNAL_WEIGHTS: Dict[str, float] = {
    "trend": 0.20,
    "momentum": 0.20,
    "volume": 0.15,
    "order_book": 0.15,
    "volatility": 0.10,
    "price_structure": 0.10,
    "news": 0.05,
    "ml": 0.05,
}

_SCORE_DIMENSIONS = tuple(DEFAULT_SIGNAL_WEIGHTS)


@dataclass(frozen=True)
class StrategyConfig:
    enabled: bool = False                    # master switch; false = not eligible
    # Lowercase regime names (e.g. "trending_up"). Empty = all regimes allowed.
    allowed_regimes: Tuple[str, ...] = ()
    signal_weights: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_SIGNAL_WEIGHTS)
    )
    long_threshold: float = 60.0            # score >= long -> LONG
    short_threshold: float = 40.0           # score <= short -> SHORT
    min_news_relevance: float = 0.0

    def __post_init__(self) -> None:
        missing = set(_SCORE_DIMENSIONS) - set(self.signal_weights)
        if missing:
            raise ConfigurationError(
                f"signal_weights missing dimensions: {sorted(missing)}"
            )
        total = sum(self.signal_weights.values())
        if abs(total - 1.0) > 1e-9:
            raise ConfigurationError(f"signal_weights must sum to 1.0, got {total:.4f}")
        if not (0.0 < self.short_threshold < self.long_threshold < 100.0):
            raise ConfigurationError(
                "need 0 < short_threshold < long_threshold < 100, got "
                f"short={self.short_threshold}, long={self.long_threshold}"
            )
        known = {r.name.lower() for r in Regime}
        for r in self.allowed_regimes:
            if r.lower() not in known:
                raise ConfigurationError(f"unknown regime in allowed_regimes: {r!r}")