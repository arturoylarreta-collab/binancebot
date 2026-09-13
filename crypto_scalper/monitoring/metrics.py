"""In-process metrics registry.

Lightweight, lock-protected counters / gauges / histograms. Exported to a
real observability backend in a later phase; for now they back periodic
health summaries and tests.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=10000)
        )

    def incr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] += by

    def decr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] -= by

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    def record(self, name: str, value: float) -> None:
        with self._lock:
            self._histograms[name].append(float(value))

    def counter(self, name: str) -> int:
        with self._lock:
            return self._counters[name]

    def gauge(self, name: str) -> Optional[float]:
        with self._lock:
            return self._gauges.get(name)

    def histogram_snapshot(self, name: str) -> List[float]:
        with self._lock:
            return list(self._histograms[name])

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            out: Dict[str, object] = {}
            for k, v in sorted(self._counters.items()):
                out[f"counter.{k}"] = v
            for k, v in sorted(self._gauges.items()):
                out[f"gauge.{k}"] = v
            for k, v in self._histograms.items():
                if v:
                    sorted_v = sorted(v)
                    n = len(sorted_v)
                    out[f"hist.{k}.p50"] = sorted_v[n // 2]
                    out[f"hist.{k}.p95"] = sorted_v[int(n * 0.95) - 1]
                    out[f"hist.{k}.p99"] = sorted_v[int(n * 0.99) - 1]
                    out[f"hist.{k}.count"] = n
            return out


METRICS = Metrics()