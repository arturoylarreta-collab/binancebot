"""Internal-vs-venue reconciliation (FASE 6).

Periodically compares the runtime state (positions + protection orders the
PositionManager believes exist) against what the adapter actually reports.
Never assumes internal state is correct: missing positions, missing stops,
missing take-profits and orphan orders are surfaced as typed issues.

`assert_clean()` raises ReconciliationMismatchError (Fatal) while
`anomalies()` stays non-raising for diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple

from crypto_scalper.core.enums import PositionStatus
from crypto_scalper.core.exceptions import ReconciliationMismatchError
from crypto_scalper.execution.base import ExchangeAdapter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager


@dataclass(frozen=True)
class ReconciliationIssue:
    kind: str          # POSITION_MISSING | QTY_MISMATCH | MISSING_STOP | MISSING_TP | ORPHAN_ORDER | UNTRACKED_POSITION
    symbol: str
    detail: str
    severity: str = "fatal"   # fatal | warning

    def __str__(self) -> str:
        return f"[{self.kind}] {self.symbol}: {self.detail}"


@dataclass
class ReconciliationReport:
    issues: Tuple[ReconciliationIssue, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return not self.issues

    @property
    def fatal_issues(self) -> Tuple[ReconciliationIssue, ...]:
        return tuple(i for i in self.issues if i.severity == "fatal")


def _qty_close(a: float, b: float, eps: float = 1e-6) -> bool:
    return abs(a - b) <= eps


class ReconciliationEngine:
    """Stateless comparator; produce a fresh report on every call."""

    async def reconcile(
        self,
        position_manager: PositionManager,
        order_manager: OrderManager,
        adapter: ExchangeAdapter,
    ) -> ReconciliationReport:
        issues = list(await self._reconcile_positions(position_manager, adapter))
        issues += await self._reconcile_protection(position_manager, adapter)
        issues += await self._reconcile_orphans(position_manager, adapter)
        return ReconciliationReport(tuple(sorted(issues, key=lambda i: i.symbol)))

    @staticmethod
    def assert_clean(report: ReconciliationReport) -> None:
        if not report.ok:
            raise ReconciliationMismatchError(
                f"reconciliation failed: "
                + "; ".join(str(i) for i in report.issues)
            )

    # ── internals ────────────────────────────────────────────────────────────

    async def _reconcile_positions(
        self,
        position_manager: PositionManager,
        adapter: ExchangeAdapter,
    ) -> Tuple[ReconciliationIssue, ...]:
        venue = {d["symbol"]: (d["side"], d["quantity"]) for d in await adapter.open_positions()}
        issues: list = []
        for pos in position_manager.open_positions():
            if pos.symbol not in venue:
                issues.append(ReconciliationIssue(
                    "POSITION_MISSING", pos.symbol,
                    f"internal {pos.side} {pos.quantity} not found on venue",
                ))
                continue
            venue_side, venue_qty = venue[pos.symbol]
            same_side = (venue_side == "LONG") == (pos.side == "BUY")
            if not same_side or not _qty_close(venue_qty, pos.quantity):
                issues.append(ReconciliationIssue(
                    "QTY_MISMATCH", pos.symbol,
                    f"internal {pos.side} {pos.quantity} vs venue {venue_side} {venue_qty}",
                ))
        internal_symbols = {p.symbol for p in position_manager.open_positions()}
        for symbol, (vside, vqty) in venue.items():
            if symbol not in internal_symbols:
                issues.append(ReconciliationIssue(
                    "UNTRACKED_POSITION", symbol,
                    f"venue holds {vside} {vqty} with no internal position",
                    severity="warning",
                ))
        return tuple(issues)

    async def _reconcile_protection(
        self,
        position_manager: PositionManager,
        adapter: ExchangeAdapter,
    ) -> Tuple[ReconciliationIssue, ...]:
        venue_open = {r.client_order_id: r for r in await adapter.open_orders()}
        issues: list = []
        for pos in position_manager.positions():
            if pos.status is not PositionStatus.ACTIVE:
                continue
            if pos.stop_client_order_id not in venue_open:
                issues.append(ReconciliationIssue(
                    "MISSING_STOP", pos.symbol,
                    f"position {pos.position_id} has no resting stop-loss",
                ))
            if pos.take_profit_client_order_id not in venue_open:
                issues.append(ReconciliationIssue(
                    "MISSING_TP", pos.symbol,
                    f"position {pos.position_id} has no resting take-profit",
                ))
        return tuple(issues)

    async def _reconcile_orphans(
        self,
        position_manager: PositionManager,
        adapter: ExchangeAdapter,
    ) -> Tuple[ReconciliationIssue, ...]:
        tracked: set = set()
        for pos in position_manager.positions():
            if pos.stop_client_order_id:
                tracked.add(pos.stop_client_order_id)
            if pos.take_profit_client_order_id:
                tracked.add(pos.take_profit_client_order_id)
        issues: list = []
        for report in await adapter.open_orders():
            if report.client_order_id not in tracked:
                issues.append(ReconciliationIssue(
                    "ORPHAN_ORDER", report.symbol,
                    f"resting order {report.client_order_id} is not tracked",
                    severity="warning",
                ))
        return tuple(issues)