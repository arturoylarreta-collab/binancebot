"""Storage abstraction (FASE 2 ships a no-op implementation).

Persisting every raw tick in PostgreSQL is a later-phase decision; the
pipeline only ever talks to a Repository protocol so the backends can be
swapped (in-memory, Parquet, PostgreSQL) without touching data processing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from crypto_scalper.core.models import FeatureSnapshot


class Repository(ABC):
    @abstractmethod
    async def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        raise NotImplementedError

    @abstractmethod
    async def save_event(self, table: str, payload: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class NoopRepository(Repository):
    async def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        return None

    async def save_event(self, table: str, payload: dict) -> None:
        return None

    async def close(self) -> None:
        return None

    def __repr__(self) -> str:  # pragma: no cover
        return "NoopRepository"