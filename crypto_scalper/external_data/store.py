"""Recent-news store with TTL, for data fusion.

Keeps the latest relevant NewsEvent per symbol. A news item that is not
fresh (older than TTL) is never fused into a new feature snapshot.
"""

from __future__ import annotations

import time
from typing import Dict, Optional

from crypto_scalper.core.models import NewsEvent


class NewsStore:
    def __init__(self, ttl_ms: int = 300_000, max_per_symbol: int = 50) -> None:
        self._ttl_ms = ttl_ms
        self._max_per_symbol = max_per_symbol
        self._latest: Dict[str, NewsEvent] = {}
        self._history: Dict[str, "list[NewsEvent]"] = {}

    def put(self, event: NewsEvent) -> None:
        key = event.symbol or "_market"
        self._latest[key] = event
        hist = self._history.setdefault(key, [])
        hist.append(event)
        if len(hist) > self._max_per_symbol:
            del hist[:-self._max_per_symbol]

    @property
    def ttl_ms(self) -> int:
        return self._ttl_ms

    def recent_count(self, symbol: str, now_ms: Optional[int] = None) -> int:
        now = now_ms or int(time.time() * 1000)
        return sum(
            1 for e in self._history.get(symbol, []) if now - e.timestamp_ms <= self._ttl_ms
        )

    def latest(self, symbol: str, now_ms: Optional[int] = None) -> Optional[NewsEvent]:
        now = now_ms or int(time.time() * 1000)
        event = self._latest.get(symbol)
        if event is None or now - event.timestamp_ms > self._ttl_ms:
            return None
        return event

    def mention_zscore(self, symbol: str, window_s: int = 300) -> float:
        """Crude trending detector placeholder (real z-score needs a baseline
        of historical mention counts, supplied by a Social provider)."""
        return 0.0

    def clear(self) -> None:
        self._latest.clear()
        self._history.clear()