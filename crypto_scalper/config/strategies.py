"""Strategy configuration.

FASE 3 defines signal-scoring weights and regimes here. This module exists
now so the shapes are fixed and validated before strategies are written.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from crypto_scalper.core.exceptions import ConfigurationError

# Default Signal Score components (FASE 3). These are NOT claimed optimal;
# they exist only so the pipeline can produce provenance-carrying scores.
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


@dataclass(frozen=True)
class StrategyConfig:
    enabled: bool = False                    # no strategy engine until FASE 3
    allowed_regimes: tuple = ()
    signal_weights: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_SIGNAL_WEIGHTS)
    )

    def __post_init__(self) -> None:
        if self.enabled:
            total = sum(self.signal_weights.values())
            if abs(total - 1.0) > 1e-9:
                raise ConfigurationError(
                    f"signal_weights must sum to 1.0, got {total:.4f}"
                )