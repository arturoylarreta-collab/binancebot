"""Storage abstraction.

The pipeline only ever talks to a Repository protocol so backends can be
swapped (in-memory, SQLite, PostgreSQL) without touching data processing.
FASE 2 ships a no-op implementation; FASE 7 adds an idempotent SQLite
backend for the trading audit trail (positions, orders, fills, risk
decisions and reconciliation reports).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

from crypto_scalper.core.models import (
    ExecutionReport,
    FeatureSnapshot,
    Fill,
    HeartbeatSnapshot,
    ManagedPosition,
    RiskDecision,
    Signal,
)
from crypto_scalper.execution.reconciliation import ReconciliationReport


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

    # ── FASE 7 trading audit trail ───────────────────────────────────────────

    @abstractmethod
    async def save_position(self, position: ManagedPosition, pnl_unrealized: Optional[float] = None) -> None:
        """Upsert a position, idempotent by position_id."""

    @abstractmethod
    async def save_order(self, report: ExecutionReport) -> None:
        """Upsert an order report, idempotent by client_order_id."""

    @abstractmethod
    async def save_fills(self, fills: Tuple[Fill, ...]) -> None:
        """Append fills, deduplicated within the process lifetime."""

    @abstractmethod
    async def save_reconciliation(self, report: ReconciliationReport) -> None:
        """Persist one reconciliation report for auditing."""

    @abstractmethod
    async def save_risk_decision(self, decision: RiskDecision) -> None:
        """Persist one risk decision (approved or rejected) for auditing."""

    @abstractmethod
    async def save_signal(self, signal: Signal, rejection_reason: str = "") -> None:
        """Persist one evaluated signal with optional rejection reason (audit)."""

    @abstractmethod
    async def save_heartbeat(self, snapshot: HeartbeatSnapshot) -> None:
        """Periodic bot status + risk metrics snapshot for the dashboard."""

    @abstractmethod
    async def load_positions(self) -> List[ManagedPosition]:
        """All persisted positions, sorted by opened_ts_ms."""

    @abstractmethod
    async def load_orders(self) -> List[ExecutionReport]:
        """All persisted order reports, sorted by ts_ms."""


class NoopRepository(Repository):
    """In-memory / discard backend: every method is a no-op."""

    async def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        return None

    async def save_event(self, table: str, payload: dict) -> None:
        return None

    async def close(self) -> None:
        return None

    async def save_position(self, position: ManagedPosition, pnl_unrealized: Optional[float] = None) -> None:
        return None

    async def save_order(self, report: ExecutionReport) -> None:
        return None

    async def save_fills(self, fills: Tuple[Fill, ...]) -> None:
        return None

    async def save_reconciliation(self, report: ReconciliationReport) -> None:
        return None

    async def save_risk_decision(self, decision: RiskDecision) -> None:
        return None

    async def save_signal(self, signal: Signal, rejection_reason: str = "") -> None:
        return None

    async def save_heartbeat(self, snapshot: HeartbeatSnapshot) -> None:
        return None

    async def load_positions(self) -> List[ManagedPosition]:
        return []

    async def load_orders(self) -> List[ExecutionReport]:
        return []

    def __repr__(self) -> str:  # pragma: no cover
        return "NoopRepository"