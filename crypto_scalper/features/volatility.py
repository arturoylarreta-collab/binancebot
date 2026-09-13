"""VOLATILITY-category features."""

from __future__ import annotations

import numpy as np

from crypto_scalper.features import technicals as t


def volatility_features(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    atr_period: int = 14,
) -> dict:
    atr_value = t.atr(high, low, close, atr_period)
    price = float(close[-1]) if len(close) else 0.0
    atr_pct = atr_value / price if (price and not np.isnan(atr_value)) else 0.0
    realized = t.realized_volatility(close, window=60)
    return {
        "atr": atr_value if not np.isnan(atr_value) else 0.0,
        "atr_pct": atr_pct,
        "realized_volatility": realized if not np.isnan(realized) else 0.0,
    }


def is_extreme_volatility(atr_pct: float, threshold: float = 0.03) -> bool:
    return atr_pct >= threshold