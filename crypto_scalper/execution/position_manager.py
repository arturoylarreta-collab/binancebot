"""Position manager (FASE 6) — the mandatory trade lifecycle.

Owns the irreversible sequence:

    Signal → Risk approval → Entry → Fill → Protection → Active

Hard invariant: a position is only ever ACTIVE while BOTH its stop-loss and
take-profit protection orders are resting on the exchange. If protection
cannot be confirmed the position is closed immediately by market order; if
that close also fails the manager raises PositionNotProtectedError (Fatal)
and marks itself halted.

Lifecycle (forward-only):
    ENTRY_SUBMITTED → ENTRY_FILLED → PROTECTING → ACTIVE → CLOSED
                                                         → ABORTED (protection lost)

The manager also keeps the realized-PnL bookkeeping that rebuilds the
frozen PortfolioState the RiskEngine consumes (daily PnL, consecutive
losses, halt/safe flags).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Callable, Dict, List, Optional, Tuple

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import (
    OrderStatus,
    OrderType,
    PositionStatus,
    RiskVerdict,
    Side,
    SignalType,
)
from crypto_scalper.core.exceptions import (
    ExecutionError,
    InvalidOrderError,
    PositionNotProtectedError,
    ProtectionTimeoutError,
)
from crypto_scalper.core.models import (
    ExecutionReport,
    ManagedPosition,
    OrderRequest,
    RiskDecision,
    Signal,
)
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.risk.portfolio import PortfolioState, Position

log = logging.getLogger(__name__)

_OPEN_STATUSES = {
    PositionStatus.ENTRY_FILLED,
    PositionStatus.PROTECTING,
    PositionStatus.ACTIVE,
}

_RESTING_OK = {
    OrderStatus.NEW.name,
}


def _consume_task_exception(task: asyncio.Task) -> None:  # pragma: no cover
    """Prevent 'Task exception was never retrieved' warnings."""
    if not task.cancelled():
        task.exception()  # noqa: B018 — intentionally freeze the exception


class PositionManager:
    def __init__(
        self,
        order_manager: OrderManager,
        config: Optional[ExecutionConfig] = None,
    ) -> None:
        self._om = order_manager
        self._adapter = order_manager.adapter
        self._config = config or ExecutionConfig()
        self._positions: Dict[str, ManagedPosition] = {}
        self._by_entry_cid: Dict[str, str] = {}
        self._by_protection_cid: Dict[str, str] = {}
        self._closed_callbacks: List[Callable[[ManagedPosition], None]] = []
        self._event_q: Optional[asyncio.Queue] = None
        self._task: Optional[asyncio.Task] = None

        # Account bookkeeping fed back into PortfolioState.
        self._daily_realized_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._trading_halted: bool = False
        self._safe_mode: bool = False
        # Guards against double-closing the same position.
        self._closing: set = set()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._event_q = self._adapter.subscribe()
            self._task = asyncio.create_task(self._consume())

    async def close(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None

    def on_position_closed(self, callback: Callable[[ManagedPosition], None]) -> None:
        self._closed_callbacks.append(callback)

    # ── Queries ──────────────────────────────────────────────────────────────

    def positions(self) -> Tuple[ManagedPosition, ...]:
        return tuple(self._positions.values())

    def open_positions(self) -> Tuple[ManagedPosition, ...]:
        return tuple(
            p for p in self._positions.values() if p.status in _OPEN_STATUSES
        )

    def get_position(self, position_id: str) -> Optional[ManagedPosition]:
        return self._positions.get(position_id)

    def build_portfolio_state(
        self,
        equity: float,
        *,
        initial_equity: Optional[float] = None,
        peak_equity: Optional[float] = None,
        ts_ms: Optional[int] = None,
    ) -> PortfolioState:
        """Snapshot for RiskEngine.assess() — frozen, freshly built."""
        open_positions = [
            Position(
                symbol=p.symbol,
                side=p.side,
                entry_price=p.entry_price,
                quantity=p.quantity,
                stop_loss_price=p.stop_loss_price,
                take_profit_price=p.take_profit_price,
                notional_value=p.notional_value,
                risk_amount=p.risk_amount,
                opened_ts_ms=p.opened_ts_ms,
                regime=p.regime,
            )
            for p in self._positions.values()
            if p.status in _OPEN_STATUSES
        ]
        return PortfolioState(
            equity=equity,
            initial_equity=initial_equity if initial_equity is not None else equity,
            open_positions=tuple(open_positions),
            daily_realized_pnl=self._daily_realized_pnl,
            peak_equity=peak_equity if peak_equity is not None else equity,
            consecutive_losses=self._consecutive_losses,
            trading_halted=self._trading_halted,
            safe_mode=self._safe_mode,
            ts_ms=ts_ms or int(time.time() * 1000),
        )

    # ── The critical path ────────────────────────────────────────────────────

    async def open(
        self,
        *,
        signal: Signal,
        decision: RiskDecision,
        entry_ref_price: Optional[float] = None,
        protection_timeout_s: Optional[float] = None,
        position_id: Optional[str] = None,
    ) -> ManagedPosition:
        """Executes an APPROVED RiskDecision with mandatory SL+TP protection.

        Raises PositionNotProtectedError (Fatal) if protection cannot be
        established and the protective market close also fails.
        """
        self._ensure_task_started()
        if decision.verdict != RiskVerdict.APPROVED.name:
            raise InvalidOrderError(
                f"refusing to execute non-approved decision: {decision.verdict}"
            )
        symbol = decision.symbol
        side = Side.BUY.name if signal.signal_type is SignalType.LONG else Side.SELL.name
        qty = decision.position_size
        if qty is None or qty <= 0:
            raise InvalidOrderError("approved decision has no positive position_size")
        if not decision.stop_loss_price or not decision.take_profit_price:
            raise InvalidOrderError("approved decision lacks SL/TP prices")

        # A MARKET entry needs a tradable reference price.
        self._ensure_price(symbol, entry_ref_price)

        position_id = position_id or f"pos-{uuid.uuid4().hex[:12]}"
        now_ms = decision.ts_ms or int(time.time() * 1000)
        entry_cid = f"ENTRY-{position_id.upper()}"
        sl_cid = f"SL-{position_id.upper()}"
        tp_cid = f"TP-{position_id.upper()}"

        pos = ManagedPosition(
            position_id=position_id,
            symbol=symbol,
            side=side,
            quantity=0.0,
            ordered_quantity=float(qty),
            entry_price=0.0,
            stop_loss_price=float(decision.stop_loss_price),
            take_profit_price=float(decision.take_profit_price),
            notional_value=float(decision.notional_value or qty * float(qty)),
            risk_amount=float(decision.risk_amount or 0.0),
            regime=signal.regime,
            status=PositionStatus.ENTRY_SUBMITTED,
            entry_client_order_id=entry_cid,
            stop_client_order_id=sl_cid,
            take_profit_client_order_id=tp_cid,
            opened_ts_ms=now_ms,
        )
        self._positions[position_id] = pos
        self._by_entry_cid[entry_cid] = position_id
        # Registered up-front (before any order is sent) so a protection fill
        # can never race ahead of its index entry.
        self._by_protection_cid[sl_cid] = position_id
        self._by_protection_cid[tp_cid] = position_id

        # ── 1. Entry ─────────────────────────────────────────────────────────
        entry_report = await self._submit_entry(pos, entry_cid, now_ms)
        filled_qty = entry_report.executed_quantity
        if filled_qty <= 0:
            raise ExecutionError(
                f"entry for {position_id} filled 0 units; nothing to protect"
            )
        pos.quantity = filled_qty
        pos.entry_price = entry_report.avg_price
        pos.entry_order_id = entry_report.order_id
        pos.status = PositionStatus.ENTRY_FILLED
        METRICS.incr("execution.entry_filled")

        # ── 2. Protection (mandatory) ────────────────────────────────────────
        try:
            sl_report, tp_report = await asyncio.wait_for(
                self._place_protection(pos, sl_cid, tp_cid, now_ms),
                timeout=protection_timeout_s if protection_timeout_s is not None
                else self._config.protection_timeout_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            raise ProtectionTimeoutError(
                f"SL/TP not confirmed within timeout for {position_id}"
            ) from None
        except ExecutionError as exc:
            await self._abort_unprotected(pos, exc)
            raise PositionNotProtectedError(
                f"{position_id}: protection failed -> {type(exc).__name__}: {exc}"
            ) from exc

        if sl_report.status not in _RESTING_OK or tp_report.status not in _RESTING_OK:
            reason = f"sl={sl_report.status}, tp={tp_report.status}"
            await self._abort_unprotected(pos, Exception(reason))
            raise PositionNotProtectedError(
                f"{position_id}: protection not resting -> {reason}"
            )

        pos.status = PositionStatus.ACTIVE
        METRICS.incr("execution.positions_activated")
        log.info("position active", extra={
            "position_id": position_id,
            "symbol": symbol,
            "side": side,
            "qty": filled_qty,
            "sl": pos.stop_loss_price,
            "tp": pos.take_profit_price,
        })
        return pos

    # ── Manual close / abort ─────────────────────────────────────────────────

    async def close_position(
        self,
        position_id: str,
        *,
        reason: str = "manual",
    ) -> ManagedPosition:
        """Cancel protections, close by market reduce-only, finalize."""
        pos = self._positions.get(position_id)
        if pos is None:
            raise InvalidOrderError(f"unknown position {position_id}")
        if pos.status is PositionStatus.CLOSED:
            return pos
        if position_id in self._closing:
            return pos
        self._closing.add(position_id)
        try:
            await self._cancel_protections(pos)
            exit_report = await self._send_close(pos, reason)
            self._finalize_close(pos, exit_report.avg_price, reason)
        finally:
            self._closing.discard(position_id)
        return pos

    async def cancel_protection(self, position_id: str) -> None:
        """Expose protection cancellation for tests/reconciliation."""
        pos = self._positions.get(position_id)
        if pos is None:
            raise InvalidOrderError(f"unknown position {position_id}")
        await self._cancel_protections(pos)

    # ── Event consumption ────────────────────────────────────────────────────

    async def _consume(self) -> None:
        assert self._event_q is not None
        while True:
            try:
                report: ExecutionReport = await asyncio.wait_for(
                    self._event_q.get(), timeout=0.5
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            self._handle_event(report)

    def _handle_event(self, report: ExecutionReport) -> None:
        # Only protection orders can drive lifecycle transitions here.
        position_id = self._by_protection_cid.get(report.client_order_id)
        if position_id is None:
            return
        if position_id in self._closing:
            return
        pos = self._positions.get(position_id)
        if pos is None or pos.status is PositionStatus.CLOSED \
                or pos.status is PositionStatus.ABORTED:
            return
        if report.is_terminal and report.status == OrderStatus.FILLED.name \
                and report.executed_quantity > 0:
            reason = "stop_loss" if report.client_order_id.startswith("SL-") \
                else "take_profit"
            self._finalize_close(pos, report.avg_price, reason)
        # Terminal non-fill (canceled/expired protection) on an ACTIVE position
        # means the position is now unprotected: close it immediately.
        elif pos.status is PositionStatus.ACTIVE and report.is_terminal:
            if report.status == OrderStatus.FILLED.name:
                return  # zero-qty fill noise; nothing changed
            METRICS.incr("execution.protection_lost")
            log.error("protection lost; closing", extra={"position_id": position_id})
            self._closing.add(position_id)
            task = asyncio.ensure_future(self._close_unprotected(pos))
            task.add_done_callback(_consume_task_exception)
        self._closing.discard(position_id)

    async def _close_unprotected(self, pos: ManagedPosition) -> None:
        try:
            await self._cancel_protections(pos)
            exit_report = await self._send_close(pos, "protection_lost")
            self._finalize_close(pos, exit_report.avg_price, "protection_lost")
        except Exception:  # noqa: BLE001
            self._trading_halted = True
            log.critical("unprotected position could not be closed; halted",
                         extra={"position_id": pos.position_id})
            raise PositionNotProtectedError(
                f"{pos.position_id}: unprotected and unable to close"
            ) from None

    # ── Helpers ──────────────────────────────────────────────────────────────

    async def _submit_entry(
        self,
        pos: ManagedPosition,
        entry_cid: str,
        now_ms: int,
    ) -> ExecutionReport:
        request = OrderRequest(
            symbol=pos.symbol,
            side=pos.side,
            order_type=OrderType.MARKET.name,
            quantity=pos.ordered_quantity,
            reduce_only=False,
            client_order_id=entry_cid,
            requested_ts_ms=now_ms,
        )
        try:
            report = await self._om.submit(request, wait_fill=False)
            if report.executed_quantity > 0 and not report.is_terminal:
                # Partially filled entry: cancel any remainder and protect the
                # quantity that actually executed. A full market fill is rare
                # but fine; a resting market order should never happen.
                await self._cancel_remaining(entry_cid, pos.symbol)
                METRICS.incr("execution.entry_partial")
            return report
        except ExecutionError as exc:
            last = self._om.get_order(entry_cid)
            if last is not None and last.executed_quantity > 0:
                await self._cancel_remaining(entry_cid, pos.symbol)
                METRICS.incr("execution.entry_partial")
                return last
            raise ExecutionError(
                f"entry submit failed for {pos.position_id}: {exc}"
            ) from exc

    async def _place_protection(
        self,
        pos: ManagedPosition,
        sl_cid: str,
        tp_cid: str,
        now_ms: int,
    ) -> Tuple[ExecutionReport, ExecutionReport]:
        fundamental = [pos.quantity, pos.symbol, now_ms, pos.side]
        if not all(fundamental):
            raise InvalidOrderError("cannot protect a zero-quantity position")
        sl_request = OrderRequest(
            symbol=pos.symbol,
            side="SELL" if pos.side == "BUY" else "BUY",
            order_type=OrderType.STOP_MARKET.name,
            quantity=pos.quantity,
            stop_price=pos.stop_loss_price,
            reduce_only=True,
            client_order_id=sl_cid,
            requested_ts_ms=now_ms,
        )
        tp_request = OrderRequest(
            symbol=pos.symbol,
            side="SELL" if pos.side == "BUY" else "BUY",
            order_type=OrderType.TAKE_PROFIT_MARKET.name,
            quantity=pos.quantity,
            stop_price=pos.take_profit_price,
            reduce_only=True,
            client_order_id=tp_cid,
            requested_ts_ms=now_ms,
        )
        sl_report = await self._om.submit(sl_request)
        tp_report = await self._om.submit(tp_request)
        return sl_report, tp_report

    async def _send_close(self, pos: ManagedPosition, reason: str) -> ExecutionReport:
        request = OrderRequest(
            symbol=pos.symbol,
            side="SELL" if pos.side == "BUY" else "BUY",
            order_type=OrderType.MARKET.name,
            quantity=pos.quantity,
            reduce_only=True,
            client_order_id=f"CLOSE-{pos.position_id.upper()}",
            requested_ts_ms=int(time.time() * 1000),
        )
        report = await self._om.submit(request, wait_fill=True)
        if report.executed_quantity <= 0:
            raise ExecutionError(f"close filled 0 for {pos.position_id}")
        return report

    async def _abort_unprotected(self, pos: ManagedPosition, cause: Exception) -> None:
        """Best-effort: kill resting protection, then close by market."""
        log.error("aborting unprotected position",
                  extra={"position_id": pos.position_id, "cause": type(cause).__name__})
        await self._cancel_protections(pos)
        try:
            exit_report = await self._send_close(pos, "abort")
        except Exception:  # noqa: BLE001
            self._trading_halted = True
            raise PositionNotProtectedError(
                f"{pos.position_id}: abort close failed after protection failure"
            ) from cause
        self._finalize_close(pos, exit_report.avg_price, "abort")

    async def _cancel_protections(self, pos: ManagedPosition) -> None:
        for cid in (pos.stop_client_order_id, pos.take_profit_client_order_id):
            if not cid:
                continue
            try:
                await self._adapter.cancel(pos.symbol, cid)
            except Exception:  # noqa: BLE001
                pass  # already terminal on venue
            self._by_protection_cid.pop(cid, None)

    async def _cancel_remaining(self, client_order_id: str, symbol: str) -> None:
        try:
            await self._adapter.cancel(symbol, client_order_id)
        except Exception:  # noqa: BLE001
            pass

    def _finalize_close(self, pos: ManagedPosition, exit_price: float, reason: str) -> None:
        sign = 1.0 if pos.side == "BUY" else -1.0
        realized = (exit_price - pos.entry_price) * sign * pos.quantity
        pos.realized_pnl = realized
        pos.close_reason = reason
        pos.status = PositionStatus.CLOSED
        pos.closed_ts_ms = int(time.time() * 1000)
        self._daily_realized_pnl += realized
        if realized < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0
        METRICS.incr("execution.positions_closed")
        log.info("position closed", extra={
            "position_id": pos.position_id,
            "reason": reason,
            "pnl": round(realized, 6),
        })
        # Hygiene: cancel the sibling protection that is still resting on the
        # venue after a SL/TP fill, so no residual reduce-only order can ever
        # pivot the net position on a later price move.
        if self._task is not None and not self._task.done():
            task = asyncio.ensure_future(self._cancel_protections(pos))
            task.add_done_callback(_consume_task_exception)
        for cb in self._closed_callbacks:
            cb(pos)

    def _ensure_price(self, symbol: str, entry_ref_price: Optional[float]) -> None:
        if hasattr(self._adapter, "get_price") and self._adapter.get_price(symbol) not in (None, 0):
            return
        if entry_ref_price and entry_ref_price > 0 and hasattr(self._adapter, "set_price"):
            self._adapter.set_price(symbol, entry_ref_price)
            return
        raise ExecutionError(f"no tradable price for {symbol}")

    def _ensure_task_started(self) -> None:
        if self._task is None or self._task.done():
            self._event_q = self._adapter.subscribe()
            self._task = asyncio.create_task(self._consume())