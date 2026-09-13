"""Classic technical indicators computed efficiently with NumPy.

No pandas in the hot path. Called at feature time (≈1 Hz per symbol) over
the bounded candle history. Every function is a pure function of its
arrays — easy to test and reason about.

Function-output convention: NaN when there is not enough data, so callers
can fall back without exceptions.
"""

from __future__ import annotations

import numpy as np

_EPS = 1e-12


def ema(values: np.ndarray, period: int) -> float:
    if len(values) < 1 or period < 1:
        return float("nan")
    alpha = 2.0 / (period + 1.0)
    # Seed with SMA when possible for the classic EMA convention.
    if len(values) >= period:
        seed = float(np.mean(values[:period]))
        out = seed
        for v in values[period:]:
            out = alpha * float(v) + (1 - alpha) * out
        return out
    out = float(values[0])
    for v in values[1:]:
        out = alpha * float(v) + (1 - alpha) * out
    return out


def rsi(closes: np.ndarray, period: int = 14) -> float:
    """Wilder's RSI. Returns NaN if fewer than period+1 closes."""
    if len(closes) < period + 1 or period < 1:
        return float("nan")
    delta = np.diff(closes[-period - 1 :])
    gains = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss < _EPS:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.empty_like(close)
    prev_close[0] = close[0]
    prev_close[1:] = close[:-1]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return tr


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
    """Wilder's ATR."""
    if len(close) < period + 1 or period < 1:
        return float("nan")
    tr = true_range(high, low, close)
    seed = float(np.mean(tr[:period]))
    out = seed
    for t in tr[period:]:
        out = (out * (period - 1) + float(t)) / period
    return out


def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
    """Wilder's ADX (+DI/-DI directional index)."""
    if len(close) < 2 * period or period < 1:
        return float("nan")
    up = np.diff(high)
    down = -np.diff(low)
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = true_range(high, low, close)[1:]

    atr_v = float(np.mean(tr[:period]))
    plus_s = float(np.mean(plus_dm[:period]))
    minus_s = float(np.mean(minus_dm[:period]))

    for i in range(period, len(tr)):
        atr_v = (atr_v * (period - 1) + float(tr[i])) / period
        plus_s = (plus_s * (period - 1) + float(plus_dm[i])) / period
        minus_s = (minus_s * (period - 1) + float(minus_dm[i])) / period

    plus_di = 100.0 * plus_s / (atr_v + _EPS)
    minus_di = 100.0 * minus_s / (atr_v + _EPS)
    dx = 100.0 * np.abs(plus_di - minus_di) / ((plus_di + minus_di) + _EPS)
    return float(dx)


def sma(values: np.ndarray, period: int) -> float:
    if len(values) < period or period < 1:
        return float("nan")
    return float(np.mean(values[-period:]))


def bollinger(closes: np.ndarray, period: int = 20, k: float = 2.0):
    if len(closes) < period or period < 1:
        return float("nan"), float("nan"), float("nan")
    window = closes[-period:]
    mid = float(np.mean(window))
    std = float(np.std(window))
    return mid + k * std, mid, mid - k * std


def roc(closes: np.ndarray, period: int = 10) -> float:
    if len(closes) < period + 1 or period < 1:
        return float("nan")
    prev = float(closes[-period - 1])
    if prev == 0:
        return 0.0
    return (float(closes[-1]) - prev) / prev


def session_vwap(
    ts: np.ndarray,
    typical: np.ndarray,
    volume: np.ndarray,
    day_ms: int = 86_400_000,
) -> float:
    """Cumulative typical-price VWAP reset at UTC day boundaries.

    Reset policy is explicit: new UTC day → new VWAP accumulation.
    Returns the running VWAP of the current session.
    """
    if len(ts) == 0:
        return float("nan")
    day_index = ts // day_ms
    current = day_index[-1]
    idx = np.where(day_index == current)[0]
    if len(idx) == 0:
        return float("nan")
    seg_ts = ts[idx]
    seg_typical = typical[idx]
    seg_vol = volume[idx]
    cum_vol = np.cumsum(seg_vol)
    if cum_vol[-1] < _EPS:
        return float(seg_typical[-1]) if len(seg_typical) else float("nan")
    weighted = np.cumsum(seg_typical * seg_vol)
    return float(weighted[-1] / cum_vol[-1])


def rolling_vwap(ts: np.ndarray, typical: np.ndarray, volume: np.ndarray, window_s: int) -> float:
    """VWAP over the trailing `window_s` seconds.

    `ts` must be sorted ascending (kept that way by CandleBuilder).
    """
    if len(ts) == 0:
        return float("nan")
    cutoff = ts[-1] - window_s * 1000
    idx = int(np.searchsorted(ts, cutoff, side="left"))
    seg_vol = volume[idx:]
    if len(seg_vol) == 0 or float(np.sum(seg_vol)) < _EPS:
        return float(typical[-1])
    return float(np.sum(typical[idx:] * seg_vol) / np.sum(seg_vol))


def typical_price(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    return (high + low + close) / 3.0


def realized_volatility(closes: np.ndarray, window: int = 60) -> float:
    """Std of log-returns over the last `window` closes (scalar)."""
    if len(closes) < window + 1:
        return float("nan")
    seg = closes[-window - 1 :]
    log_ret = np.diff(np.log(np.maximum(seg, _EPS)))
    return float(np.std(log_ret))