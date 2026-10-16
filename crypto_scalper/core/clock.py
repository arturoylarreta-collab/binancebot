"""Exchange-aligned wall clock.

Trade buckets, candles and depth events carry *exchange* timestamps. If the
host clock drifts (a laptop 30 s ahead is common), comparing them with
``time.time()`` makes fresh data look stale and shifts every 1s/5s feature
window. ``now_ms()`` returns host time corrected by the measured offset to
the exchange; the offset defaults to 0 (tests, offline replays).
"""

from __future__ import annotations

import time

_offset_ms: int = 0


def now_ms() -> int:
    return int(time.time() * 1000) + _offset_ms


def offset_ms() -> int:
    return _offset_ms


def set_offset_ms(value: int) -> None:
    global _offset_ms
    _offset_ms = int(value)


async def sync_with(rest) -> int:
    """Measure the offset against ``rest.server_time()`` (midpoint of the RTT)."""
    t0 = time.time()
    server = await rest.server_time()
    t1 = time.time()
    set_offset_ms(int(server) - int((t0 + t1) / 2 * 1000))
    return _offset_ms
