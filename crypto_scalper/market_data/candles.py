"""Candle aggregation from the aggTrade stream.

Buckets trades into fixed-interval candles (default 1s) and keeps a bounded
history in memory so indicators can be computed at feature time.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple

import numpy as np

from crypto_scalper.core.models import AggTrade, Candle


@dataclass
class CandleSeries:
    ts: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    quote_volume: np.ndarray
    trade_count: np.ndarray


@dataclass
class _MutableCandle:
    symbol: str
    ts_ms: int
    interval_s: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int


class CandleBuilder:
    def __init__(self, symbol: str, interval_s: int = 1, max_candles: int = 2000) -> None:
        self.symbol = symbol
        self.interval_s = interval_s
        self._max_candles = max_candles
        self._completed: Deque[Candle] = deque(maxlen=max_candles)
        self._current: Optional[_MutableCandle] = None

    def add(self, trade: AggTrade) -> None:
        now = trade.event_time_ms
        bucket_ts = (now // (self.interval_s * 1000)) * self.interval_s * 1000
        if self._current is None or self._current.ts_ms != bucket_ts:
            self._close_current(bucket_ts)
            self._current = _MutableCandle(
                symbol=self.symbol,
                ts_ms=bucket_ts,
                interval_s=self.interval_s,
                open=trade.price,
                high=trade.price,
                low=trade.price,
                close=trade.price,
                volume=0.0,
                quote_volume=0.0,
                trade_count=0,
            )
        c = self._current
        c.high = max(c.high, trade.price)
        c.low = min(c.low, trade.price)
        c.close = trade.price
        c.volume += trade.quantity
        c.quote_volume += trade.price * trade.quantity
        c.trade_count += 1

    def _close_current(self, next_bucket_ts: int) -> None:
        if self._current is not None and self._current.trade_count > 0:
            c = self._current
            self._completed.append(
                Candle(
                    symbol=c.symbol,
                    ts_ms=c.ts_ms,
                    interval_s=c.interval_s,
                    open=c.open,
                    high=c.high,
                    low=c.low,
                    close=c.close,
                    volume=c.volume,
                    quote_volume=c.quote_volume,
                    trade_count=c.trade_count,
                    completed=True,
                )
            )
        self._current = None

    def series(self, include_current: bool = True) -> CandleSeries:
        candles = list(self._completed)
        if include_current and self._current is not None and self._current.trade_count > 0:
            candles = candles + [self._current]
        if not candles:
            return CandleSeries(*[np.array([], dtype=np.float64) for _ in range(7)])
        return CandleSeries(
            ts=np.array([c.ts_ms for c in candles], dtype=np.float64),
            open=np.array([c.open for c in candles], dtype=np.float64),
            high=np.array([c.high for c in candles], dtype=np.float64),
            low=np.array([c.low for c in candles], dtype=np.float64),
            close=np.array([c.close for c in candles], dtype=np.float64),
            volume=np.array([c.volume for c in candles], dtype=np.float64),
            quote_volume=np.array([c.quote_volume for c in candles], dtype=np.float64),
            trade_count=np.array([c.trade_count for c in candles], dtype=np.float64),
        )

    @property
    def last_close(self) -> Optional[float]:
        if self._current is not None and self._current.trade_count > 0:
            return self._current.close
        if self._completed:
            return self._completed[-1].close
        return None

    @property
    def count(self) -> int:
        return len(self._completed) + (1 if self._current is not None else 0)

    @property
    def current(self) -> Optional[Candle]:
        return self._current