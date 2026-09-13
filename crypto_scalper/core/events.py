"""Event bus for the event-driven pipeline.

Producer (WebSocket/REST) -> queues -> demux -> per-symbol processors.
Consumers subscribe to a topic and receive copies; bounded queues push
backpressure upstream so a slow consumer cannot silently lose events.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from crypto_scalper.core.models import AggTrade, Candle, DiffDepthEvent, FeatureSnapshot

log = logging.getLogger(__name__)

# Topic namespace helpers
SYMBOL_TRADE = "market.{}.trade"
SYMBOL_DEPTH = "market.{}.depth"
SYMBOL_CANDLE = "market.{}.candle"
SYMBOL_FEATURE = "feature.{}"
LIFECYCLE = "lifecycle"


@dataclass
class Event:
    topic: str
    mono_ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class TradeEvent(Event):
    trade: AggTrade = None  # type: ignore


@dataclass
class DepthEvent(Event):
    diff: DiffDepthEvent = None  # type: ignore


@dataclass
class CandleEvent(Event):
    candle: Candle = None  # type: ignore


@dataclass
class FeatureEvent(Event):
    snapshot: FeatureSnapshot = None  # type: ignore


@dataclass
class LifecycleEvent(Event):
    kind: str = ""                 # e.g. ws.connected, ws.disconnected, resync
    symbol: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)


class QueueFullError(asyncio.QueueFull):
    """Raised when a bounded topic queue overflows; caller must resync."""


class EventBus:
    """Minimal pub/sub over bounded asyncio queues with topic fan-out."""

    def __init__(self, queue_maxsize: int = 10000) -> None:
        self._queue_maxsize = queue_maxsize
        self._topics: Dict[str, Set[asyncio.Queue]] = {}

    def subscribe(self, topic: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_maxsize)
        self._topics.setdefault(topic, set()).add(q)
        return q

    def unsubscribe(self, topic: str, q: asyncio.Queue) -> None:
        subs = self._topics.get(topic)
        if subs:
            subs.discard(q)
            if not subs:
                self._topics.pop(topic, None)

    def subscriber_count(self, topic: str) -> int:
        return len(self._topics.get(topic, set()))

    async def publish(self, event: Event) -> None:
        subs = list(self._topics.get(event.topic, ()))
        if not subs:
            return
        await asyncio.gather(*(_put_safe(q, event) for q in subs))

    def publish_nowait(self, event: Event) -> None:
        subs = list(self._topics.get(event.topic, ()))
        for q in subs:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # A full bounded queue is fatal to correctness for the
                # producer calling synchronously: raise so the caller can
                # drop state and resync rather than silently losing data.
                raise QueueFullError(f"queue full for topic={event.topic}")


async def _put_safe(q: asyncio.Queue, item: Any) -> None:
    await q.put(item)