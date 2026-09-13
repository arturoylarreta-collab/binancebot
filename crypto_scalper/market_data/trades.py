"""Per-symbol trade aggregation in sliding windows.

Efficient in-memory bucketing (no pandas in the hot path). One tick only
appends to the current 1-second bucket; window sums are computed at
feature time by summing the last K buckets.

Windows: 1s, 5s, 15s, 30s, 1m, 5m (configurable). Buy/sell split uses the
aggressor side from aggTrades (m = buyer is maker).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.models import AggTrade
from crypto_scalper.monitoring.metrics import Metrics

DEFAULT_WINDOWS_S = (1, 5, 15, 30, 60, 300)
_ZSCORE_HISTORY = 500


@dataclass
class _Bucket:
    ts_s: int
    count: int = 0
    buy_qty: float = 0.0
    sell_qty: float = 0.0
    total_qty: float = 0.0
    quote_volume: float = 0.0


@dataclass
class WindowStats:
    window_s: int
    ts_ms: int
    count: int
    buy_qty: float
    sell_qty: float
    volume: float           # buy_qty + sell_qty (base)
    quote_volume: float
    avg_trade_size: float   # base qty per trade
    buy_ratio: float        # buy_qty / volume


class TradeAggregator:
    def __init__(
        self,
        windows_s: tuple = DEFAULT_WINDOWS_S,
        zscore_history: int = _ZSCORE_HISTORY,
        metrics: Optional[Metrics] = None,
        symbol: str = "",
    ) -> None:
        self._windows = windows_s
        self._max_window_s = max(windows_s)
        self._zscore_history = zscore_history
        self._metrics = metrics
        self._symbol = symbol
        self._buckets: Deque[_Bucket] = deque()
        self._active: _Bucket | None = None
        # per-window historical volumes for z-score / relative volume
        self._history: Dict[int, Deque[float]] = {
            w: deque(maxlen=zscore_history) for w in windows_s
        }
        self._last_volume: Dict[int, float] = {}
        self._trades_seen: int = 0

    @property
    def trades_seen(self) -> int:
        return self._trades_seen

    def add(self, trade: AggTrade, now_ms: int) -> None:
        bucket_ts = now_ms // 1000
        if self._active is None or self._active.ts_s != bucket_ts:
            self._rotate(bucket_ts, now_ms)
        self._active.count += 1
        self._active.total_qty += trade.quantity
        self._active.quote_volume += trade.price * trade.quantity
        if trade.aggressor is AggressorSide.BUY:
            self._active.buy_qty += trade.quantity
        elif trade.aggressor is AggressorSide.SELL:
            self._active.sell_qty += trade.quantity
        self._trades_seen += 1
        self._trim(now_ms)

    def _rotate(self, bucket_ts: int, now_ms: int) -> None:
        if self._active is not None:
            self._buckets.append(self._active)
        self._active = _Bucket(ts_s=bucket_ts)
        self._trim(now_ms)

    def _trim(self, now_ms: int) -> None:
        cutoff = now_ms // 1000 - self._max_window_s - 1
        while self._buckets and self._buckets[0].ts_s < cutoff:
            if self._metrics:
                self._metrics.decr("trades.buckets_dropped")  # placeholder metric
            self._buckets.popleft()

    def _window(self, window_s: int, now_ms: int) -> WindowStats:
        now_s = now_ms // 1000
        cutoff = now_s - window_s
        count = 0
        buy = 0.0
        sell = 0.0
        total = 0.0
        quote = 0.0
        for b in self._buckets:
            if b.ts_s <= cutoff:
                continue
            count += b.count
            buy += b.buy_qty
            sell += b.sell_qty
            total += b.total_qty
            quote += b.quote_volume
        if self._active is not None and self._active.ts_s > cutoff:
            count += self._active.count
            buy += self._active.buy_qty
            sell += self._active.sell_qty
            total += self._active.total_qty
            quote += self._active.quote_volume
        volume = total
        avg = total / count if count else 0.0
        buy_ratio = buy / volume if volume else 0.0
        return WindowStats(
            window_s=window_s, ts_ms=now_ms, count=count,
            buy_qty=buy, sell_qty=sell, volume=volume,
            quote_volume=quote, avg_trade_size=avg, buy_ratio=buy_ratio,
        )

    def metrics(self, now_ms: int) -> Dict[str, WindowStats]:
        return {w: self._window(w, now_ms) for w in self._windows}

    def volume_metrics(self, now_ms: int, window_s: int = 60) -> Dict[str, float]:
        """volume, relative_volume (vs EWMA baseline), z-score, acceleration."""
        stats = self._window(window_s, now_ms)
        vol = stats.volume
        hist = self._history[window_s]
        if len(hist) >= 2 and vol > 0:
            hist.append(vol)
        elif vol > 0:
            hist.append(vol)

        mean = _mean(hist)
        std = _std(hist, mean)
        baseline = _ewma(hist)

        zscore = (vol - mean) / std if std > 1e-12 else 0.0
        relative_volume = vol / baseline if baseline > 1e-12 else 0.0
        acceleration = vol - self._last_volume.get(window_s, vol)
        self._last_volume[window_s] = vol

        return {
            "volume": vol,
            "relative_volume": relative_volume,
            "volume_zscore": zscore,
            "volume_acceleration": acceleration,
            "baseline": baseline,
        }


def _mean(values: Deque[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std(values: Deque[float], mean: float) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / n
    return var ** 0.5


def _ewma(values: Deque[float], alpha: float = 0.2) -> float:
    if not values:
        return 0.0
    ewma = values[0]
    for v in list(values)[1:]:
        ewma = alpha * v + (1 - alpha) * ewma
    return ewma