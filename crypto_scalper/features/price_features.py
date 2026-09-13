"""PRICE-category features (pure functions over candle history)."""

from __future__ import annotations

import numpy as np

from crypto_scalper.features import technicals as t


def price_features(close: np.ndarray, high: np.ndarray, low: np.ndarray) -> dict:
    n = len(close)
    out = {
        "price": float(close[-1]) if n else 0.0,
        "return_1s": _ret(close, 1),
        "return_5s": _ret(close, 5),
        "return_15s": _ret(close, 15),
        "return_60s": _ret(close, 60),
        "roc_10": t.roc(close, 10),
        "range_1s_pct": _range_pct(high, low, 1),
        "price_vs_ema9_pct": _vs_ema(close, 9),
    }
    return out


def _ret(close: np.ndarray, n: int) -> float:
    if len(close) < n + 1 or float(close[-n - 1]) == 0:
        return 0.0
    return float(close[-1] / close[-n - 1] - 1.0)


def _range_pct(high: np.ndarray, low: np.ndarray, n: int) -> float:
    if len(high) < n + 1:
        return 0.0
    seg_h = float(np.max(high[-n - 1 :]))
    seg_l = float(np.min(low[-n - 1 :]))
    mid = (seg_h + seg_l) / 2.0
    if mid == 0:
        return 0.0
    return (seg_h - seg_l) / mid


def _vs_ema(close: np.ndarray, period: int) -> float:
    ema = t.ema(close, period)
    if np.isnan(ema) or ema == 0:
        return 0.0
    return float(close[-1] / ema - 1.0)