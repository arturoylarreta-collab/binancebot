"""PeriodicReconciler (FASE 7) — internal-vs-venue drift patrol.

Runs ReconciliationEngine on a cadence and mitigates the actionable kinds
instead of only complaining:

  MISSING_STOP / MISSING_TP  → close the affected ACTIVE position by market
                               (never trade unprotected);
  ORPHAN_ORDER               → cancel the resting order;
  UNTRACKED_POSITION         → flatten the venue exposure with a reduce-only
                               market order.

After mitigation the venue is re-checked: anything still fatal
(POSITION_MISSING / QTY_MISMATCH / an unresolvable protection gap) pauses
the bot via the `set_paused` callback. Every report is persisted for audit.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Callable, Optional

from crypto_scalper.core.enums import OrderType, PositionStatus, Side
from crypto_scalper.core.models import OrderRequest
from crypto_scalper.execution.base import ExchangeAdapter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.reconciliation import (
    BOT_ORDER_PREFIXES,
    ReconciliationEngine,
    ReconciliationReport,
)
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.storage.base import NoopRepository, Repository

log = logging.getLogger(__name__)

_MISSING_PROTECTION = frozenset({"MISSING_STOP", "MISSING_TP"})
_FATAL_ACTIONABLE = frozenset({"POSITION_MISSING", "QTY_MISMATCH"})
_TRANSITIONAL = frozenset({
    PositionStatus.ENTRY_SUBMITTED, PositionStatus.ENTRY_FILLED, PositionStatus.PROTECTING,
})


class PeriodicReconciler:
    def __init__(
        self,
        position_manager: PositionManager,
        order_manager: OrderManager,
        adapter: ExchangeAdapter,
        *,
        interval_s: float = 60.0,
        repository: Optional[Repository] = None,
        set_paused: Optional[Callable[[str], None]] = None,
        managed_symbols: Optional[set] = None,
    ) -> None:
        self._managed_symbols = {s.upper() for s in managed_symbols} if managed_symbols else None
        self._pm = position_manager
        self._om = order_manager
        self._adapter = adapter
        self._interval_s = float(interval_s)
        self._repo = repository or NoopRepository()
        self._set_paused = set_paused
        self._engine = ReconciliationEngine()

        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._reconcile_count = 0
        self._last_report: Optional[ReconciliationReport] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None

    # ── public ───────────────────────────────────────────────────────────────

    @property
    def reconcile_count(self) -> int:
        return self._reconcile_count

    @property
    def last_report(self) -> Optional[ReconciliationReport]:
        return self._last_report

    async def reconcile_once(self) -> ReconciliationReport:
        """Reconcile, mitigate actionable drift, re-check and persist."""
        if self._busy_symbols():
            # A position is mid-open (entry/protection in flight): venue and
            # book legitimately disagree for a moment. Check again next pass.
            METRICS.incr("paper.reconcile_deferred")
            report = await self._engine.reconcile(self._pm, self._om, self._adapter)
            busy = self._busy_symbols()
            report = type(report)(tuple(i for i in report.issues if i.symbol not in busy))
            self._last_report = report
            return report
        report = await self._engine.reconcile(self._pm, self._om, self._adapter)
        self._reconcile_count += 1
        if not report.ok:
            log.warning("reconciliation drift", extra={"issues": len(report.issues)})
            for issue in report.issues:
                log.warning("reconciliation issue",
                            extra={"kind": issue.kind, "symbol": issue.symbol,
                                   "detail": issue.detail})
            await self._mitigate(report)
            report = await self._engine.reconcile(self._pm, self._om, self._adapter)
            self._reconcile_count += 1

        self._last_report = report
        await self._repo.save_reconciliation(report)
        if report.fatal_issues:
            detail = "; ".join(str(i) for i in report.fatal_issues)
            METRICS.incr("paper.reconciliation_fatal")
            if self._set_paused is not None:
                self._set_paused(f"reconciliation_fatal:{detail}")
        else:
            METRICS.incr("paper.reconciliation_ok")
        return report

    # ── internals ────────────────────────────────────────────────────────────

    def _busy_symbols(self) -> set:
        return {p.symbol for p in self._pm.positions() if p.status in _TRANSITIONAL}

    def _is_ours(self, client_order_id: str, symbol: str) -> bool:
        if self._managed_symbols is not None and symbol.upper() not in self._managed_symbols:
            return False
        return client_order_id.startswith(BOT_ORDER_PREFIXES)

    async def _run(self) -> None:
        # First pass immediately, then on the configured cadence. A failing
        # pass (network, venue hiccup) must never kill the patrol.
        try:
            await self.reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("initial reconciliation failed")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_s)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self.reconcile_once()
            except Exception:  # noqa: BLE001
                log.exception("periodic reconciliation failed")

    async def _mitigate(self, report: ReconciliationReport) -> None:
        kinds = {i.kind for i in report.issues}
        if kinds & _MISSING_PROTECTION:
            affected_symbols = {i.symbol for i in report.issues
                                if i.kind in _MISSING_PROTECTION}
            await self._close_unprotected(affected_symbols)
        if report.issues:
            await self._cancel_orphans()
            await self._flatten_untracked()

    async def _close_unprotected(self, symbols: set) -> None:
        for pos in list(self._pm.open_positions()):
            if pos.symbol in symbols and pos.status is PositionStatus.ACTIVE:
                try:
                    await self._pm.close_position(
                        pos.position_id, reason="reconcile_missing_protection"
                    )
                    METRICS.incr("paper.reconcile_closed_position")
                except Exception:  # noqa: BLE001
                    log.exception("reconcile close failed",
                                  extra={"position_id": pos.position_id})

    async def _cancel_orphans(self) -> None:
        tracked = set()
        for pos in self._pm.open_positions():
            if pos.stop_client_order_id:
                tracked.add(pos.stop_client_order_id)
            if pos.take_profit_client_order_id:
                tracked.add(pos.take_profit_client_order_id)
        busy = self._busy_symbols()
        for report in await self._adapter.open_orders():
            if report.symbol in busy or not self._is_ours(report.client_order_id, report.symbol):
                continue
            if report.client_order_id not in tracked:
                try:
                    await self._adapter.cancel(report.symbol, report.client_order_id)
                    METRICS.incr("paper.reconcile_cancelled_orphan")
                except Exception:  # noqa: BLE001
                    log.warning("orphan cancel failed",
                                extra={"client_order_id": report.client_order_id})

    async def _flatten_untracked(self) -> None:
        internal = {p.symbol for p in self._pm.positions()
                    if p.status in _TRANSITIONAL or p.status is PositionStatus.ACTIVE}
        for venue_pos in await self._adapter.open_positions():
            symbol = venue_pos["symbol"]
            if symbol in internal:
                continue
            if self._managed_symbols is not None and symbol not in self._managed_symbols:
                continue  # not ours to flatten
            qty = float(venue_pos["quantity"])
            side = Side.SELL.name if venue_pos["side"] == "LONG" else Side.BUY.name
            try:
                await self._om.submit(OrderRequest(
                    symbol=symbol,
                    side=side,
                    order_type=OrderType.MARKET.name,
                    quantity=qty,
                    reduce_only=True,
                    client_order_id=f"FLAT-{symbol}-{uuid.uuid4().hex[:8]}",
                    requested_ts_ms=int(time.time() * 1000),
                ), wait_fill=True)
                METRICS.incr("paper.reconcile_flattened_untracked")
            except Exception:  # noqa: BLE001
                log.exception("flatten untracked failed", extra={"symbol": symbol})