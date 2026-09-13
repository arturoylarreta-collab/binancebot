"""External data plugin architecture (news / social / trending).

The strategy and feature engines only know about the Provider protocol and
NewsEvent; swapping CryptoPanic/LunarCrush/GDELT/etc. requires no changes
outside this package. FASE 2 ships providers that produce nothing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Optional

from crypto_scalper.core.enums import Impact
from crypto_scalper.core.models import NewsEvent

_IMPACT_KEYWORDS_HIGH = ("hack", "exploit", "sec", "lawsuit", "ban", "halving",
                         "etf", "approval", "delist", "default", "crash")
_IMPACT_KEYWORDS_MEDIUM = ("partnership", "upgrade", "launch", "listing",
                           "fork", "inflation", "regulation", "leverage")


class NewsProvider(ABC):
    """Streams normalized NewsEvent objects."""

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def next_events(self) -> AsyncIterator[NewsEvent]:
        """Yield normalized NewsEvent objects; blocks until the next event."""
        raise NotImplementedError
        yield  # pragma: no cover


def classify_impact(headline: str) -> Impact:
    h = headline.lower()
    if any(k in h for k in _IMPACT_KEYWORDS_HIGH):
        return Impact.HIGH
    if any(k in h for k in _IMPACT_KEYWORDS_MEDIUM):
        return Impact.MEDIUM
    return Impact.LOW


def symbol_relevance(headline: str, symbol: str, base_asset: str) -> float:
    """Heuristic [0,1] relevance of a headline to a symbol."""
    h = headline.lower()
    score = 0.0
    if symbol.lower() in h:
        score += 0.6
    if base_asset and base_asset.lower() in h:
        score += 0.4
    return max(0.0, min(1.0, score))


class NullProvider(NewsProvider):
    """Default provider: the pipeline runs without external data."""

    def name(self) -> str:
        return "null"

    async def next_events(self) -> AsyncIterator[NewsEvent]:
        while True:
            await _never()
            yield None


async def _never():
    import asyncio
    await asyncio.sleep(3600.0)
    return False


def build_providers(configured: Optional[List[str]] = None) -> List[NewsProvider]:
    """Instantiate providers by name; unknown names raise at startup."""
    if not configured:
        return [NullProvider()]
    providers = []
    for name in configured:
        if name == "null":
            providers.append(NullProvider())
        else:
            # No other providers are implemented in FASE 2. Fail fast rather
            # than silently trading without data.
            raise ValueError(f"unknown news provider: {name}")
    return providers