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
from datetime import datetime, timezone
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
    AlreadyFlatError,
    ExecutionError,
    InvalidOrderError,
    OrderNotFoundError,
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
    """Log background failures instead of 'Task exception was never retrieved'."""
    if not task.cancelled() and task.exception() is not None:
        log.error("background execution task failed",
                  exc_info=task.exception())


def _utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


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

        # Account bookkeeping fed back into PortfolioState. Daily PnL is NET
        # of fees and rolls over at 00:00 UTC.
        self._daily_realized_pnl: float = 0.0
        self._day: str = _utc_day(int(time.time() * 1000))
        self._consecutive_losses: int = 0
        self._last_loss_ms: int = 0
        self._trading_halted: bool = False
        self._safe_mode: bool = False
        # Guards against double-closing the same position.
        self._closing: set = set()
        self._bg_tasks: set = set()
        self._close_seq: Dict[str, int] = {}
        # Per-symbol exchange grid (set by the ExecutionRouter when available).
        self.filters = None

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._ensure_task_started()

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

    @property
    def trading_halted(self) -> bool:
        return self._trading_halted

    @property
    def daily_realized_pnl(self) -> float:
        return self._daily_realized_pnl

    @property
    def consecutive_losses(self) -> int:
        return self._consecutive_losses

    def reset_halt(self) -> None:
        """Operator action (dashboard / restart): clear halt + loss streak."""
        self._trading_halted = False
        self._consecutive_losses = 0

    def _roll_day(self, now_ms: int) -> None:
        day = _utc_day(now_ms)
        if day != self._day:
            log.info("daily pnl rollover", extra={"day": day,
                                                  "prev_pnl": round(self._daily_realized_pnl, 4)})
            self._day = day
            self._daily_realized_pnl = 0.0

    def _decay_loss_streak(self, now_ms: int) -> None:
        cooldown_ms = int(self._config.loss_streak_cooldown_s * 1000)
        if (cooldown_ms > 0 and self._consecutive_losses > 0 and self._last_loss_ms
                and now_ms - self._last_loss_ms >= cooldown_ms):
            log.info("loss streak cooled down", extra={"losses": self._consecutive_losses})
            self._consecutive_losses = 0

    def build_portfolio_state(
        self,
        equity: float,
        *,
        initial_equity: Optional[float] = None,
        peak_equity: Optional[float] = None,
        ts_ms: Optional[int] = None,
    ) -> PortfolioState:
        """Snapshot for RiskEngine.assess() — frozen, freshly built."""
        now_ms = ts_ms or int(time.time() * 1000)
        self._roll_day(now_ms)
        self._decay_loss_streak(now_ms)
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
            notional_value=float(decision.notional_value
                                 or qty * float(entry_ref_price or 0.0)),
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
        try:
            entry_report = await self._submit_entry(pos, entry_cid, now_ms)
        except BaseException:
            self._discard_failed(pos)
            raise
        filled_qty = entry_report.executed_quantity
        if filled_qty <= 0:
            self._discard_failed(pos)
            raise ExecutionError(
                f"entry for {position_id} filled 0 units; nothing to protect"
            )
        pos.quantity = filled_qty
        pos.entry_price = entry_report.avg_price
        pos.entry_order_id = entry_report.order_id
        pos.fees += self._fees_of(entry_report)
        pos.status = PositionStatus.ENTRY_FILLED
        METRICS.incr("execution.entry_filled")

        # ── 2. Protection (mandatory) ────────────────────────────────────────
        # SL/TP are re-anchored to the ACTUAL fill price (keeping the approved
        # distances) so slippage can never put the stop on the wrong side.
        self._reanchor_protection(pos, entry_ref_price)
        pos.status = PositionStatus.PROTECTING
        timeout_s = (protection_timeout_s if protection_timeout_s is not None
                     else self._config.protection_timeout_s)
        try:
            sl_report, tp_report = await asyncio.wait_for(
                self._place_protection(pos, sl_cid, tp_cid, now_ms), timeout=timeout_s,
            )
        except asyncio.CancelledError:
            await self._abort_unprotected(pos, TimeoutError("cancelled while protecting"))
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            # Never leave a filled position without SL/TP: abort it now.
            await self._abort_unprotected(pos, exc)
            raise ProtectionTimeoutError(
                f"SL/TP not confirmed within {timeout_s}s for {position_id}; closed"
            ) from None
        except Exception as exc:  # noqa: BLE001 - ANY failure means unprotected
            if pos.status is PositionStatus.CLOSED:
                return pos  # a protection already filled and closed it
            await self._abort_unprotected(pos, exc)
            raise PositionNotProtectedError(
                f"{position_id}: protection failed -> {type(exc).__name__}: {exc}"
            ) from exc

        if pos.status is PositionStatus.CLOSED:
            # SL/TP filled while the sibling was still being placed.
            return pos
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
            if exit_report is not None:
                self._finalize_close(pos, exit_report.avg_price, reason, exit_report)
            else:
                self._finalize_close(pos, self._last_price(pos), f"{reason}_already_flat")
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
            try:
                self._om.record(report)
                self._handle_event(report)
            except Exception:  # noqa: BLE001 - the consumer must never die
                METRICS.incr("execution.event_errors")
                log.exception("execution event handling failed",
                              extra={"client_order_id": report.client_order_id})

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
        if report.is_terminal and report.executed_quantity > 0 and report.status in (
                OrderStatus.FILLED.name, OrderStatus.PARTIALLY_FILLED_CANCELED.name):
            reason = "stop_loss" if report.client_order_id.startswith("SL-") \
                else "take_profit"
            if report.executed_quantity + 1e-12 < pos.quantity:
                # partial protection fill: close the remainder, never leave it naked
                pos.quantity -= report.executed_quantity
                self._spawn_close_unprotected(pos, f"{reason}_partial")
                return
            self._finalize_close(pos, report.avg_price, reason, report)
        # Terminal non-fill (canceled/expired protection) on an ACTIVE position
        # means the position is now unprotected: close it immediately.
        elif pos.status is PositionStatus.ACTIVE and report.is_terminal:
            if report.status == OrderStatus.FILLED.name:
                return  # zero-qty fill noise; nothing changed
            METRICS.incr("execution.protection_lost")
            log.error("protection lost; closing", extra={"position_id": position_id,
                                                        "status": report.status})
            self._spawn_close_unprotected(pos, "protection_lost")

    def _spawn_close_unprotected(self, pos: ManagedPosition, reason: str) -> None:
        self._closing.add(pos.position_id)
        task = asyncio.ensure_future(self._close_unprotected(pos, reason))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        task.add_done_callback(_consume_task_exception)

    async def _close_unprotected(self, pos: ManagedPosition, reason: str = "protection_lost") -> None:
        try:
            await self._cancel_protections(pos)
            exit_report = await self._send_close(pos, reason)
            if exit_report is not None:
                self._finalize_close(pos, exit_report.avg_price, reason, exit_report)
            else:
                self._finalize_close(pos, self._last_price(pos), f"{reason}_already_flat")
        except Exception:  # noqa: BLE001
            self._trading_halted = True
            log.critical("unprotected position could not be closed; halted",
                         extra={"position_id": pos.position_id})
            raise PositionNotProtectedError(
                f"{pos.position_id}: unprotected and unable to close"
            ) from None
        finally:
            self._closing.discard(pos.position_id)

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
            if not report.is_terminal and report.executed_quantity <= 0:
                # Live venues may ACK a MARKET order before it matches.
                report = await self._await_entry_fill(pos.symbol, entry_cid, report)
            if report.executed_quantity > 0 and not report.is_terminal:
                # Partially filled entry: cancel any remainder and protect the
                # quantity that actually executed.
                await self._cancel_remaining(entry_cid, pos.symbol)
                report = await self._venue_report(pos.symbol, entry_cid) or report
                METRICS.incr("execution.entry_partial")
            return report
        except Exception as exc:  # noqa: BLE001 - live adapters raise ExchangeError too
            # The OrderManager cache is only written on success: ask the venue
            # what really happened before declaring the entry failed.
            last = await self._venue_report(pos.symbol, entry_cid) or self._om.get_order(entry_cid)
            if last is not None and last.executed_quantity > 0:
                await self._cancel_remaining(entry_cid, pos.symbol)
                METRICS.incr("execution.entry_partial")
                return last
            raise ExecutionError(
                f"entry submit failed for {pos.position_id}: {type(exc).__name__}: {exc}"
            ) from exc

    async def _await_entry_fill(self, symbol: str, cid: str,
                                report: ExecutionReport) -> ExecutionReport:
        deadline = time.monotonic() + self._config.entry_fill_timeout_s
        poll = 0.05
        while time.monotonic() < deadline:
            await asyncio.sleep(poll)
            poll = min(poll * 2, 0.5)
            latest = await self._venue_report(symbol, cid)
            if latest is not None:
                report = latest
                if report.is_terminal or report.executed_quantity > 0:
                    return report
        await self._cancel_remaining(cid, symbol)
        return await self._venue_report(symbol, cid) or report

    async def _venue_report(self, symbol: str, cid: str) -> Optional[ExecutionReport]:
        try:
            return await self._adapter.get_order(symbol, cid)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            return None

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

    async def _send_close(self, pos: ManagedPosition, reason: str) -> Optional[ExecutionReport]:
        """Reduce-only market close. Returns None when the venue says the
        position is already flat (e.g. a protection filled concurrently)."""
        # A fresh client id per attempt: Binance accepts a reused id once the
        # previous order is terminal, and the OrderManager would otherwise
        # replay a stale (canceled, zero-fill) report.
        seq = self._close_seq.get(pos.position_id, 0) + 1
        self._close_seq[pos.position_id] = seq
        suffix = "" if seq == 1 else f"-{seq}"
        request = OrderRequest(
            symbol=pos.symbol,
            side="SELL" if pos.side == "BUY" else "BUY",
            order_type=OrderType.MARKET.name,
            quantity=pos.quantity,
            reduce_only=True,
            client_order_id=f"CLOSE-{pos.position_id.upper()}{suffix}",
            requested_ts_ms=int(time.time() * 1000),
        )
        try:
            report = await self._om.submit(request, wait_fill=True)
        except AlreadyFlatError:
            log.warning("close found position already flat",
                        extra={"position_id": pos.position_id, "reason": reason})
            return None
        if report.executed_quantity <= 0:
            raise ExecutionError(f"close filled 0 for {pos.position_id}")
        return report

    async def _abort_unprotected(self, pos: ManagedPosition, cause: Exception) -> None:
        """Best-effort: kill resting protection, then close by market."""
        log.error("aborting unprotected position",
                  extra={"position_id": pos.position_id, "cause": type(cause).__name__})
        if pos.status is PositionStatus.CLOSED:
            return
        self._closing.add(pos.position_id)
        try:
            await self._cancel_protections(pos)
            try:
                exit_report = await self._send_close(pos, "abort")
            except Exception:  # noqa: BLE001
                self._trading_halted = True
                raise PositionNotProtectedError(
                    f"{pos.position_id}: abort close failed after protection failure"
                ) from cause
            if exit_report is not None:
                self._finalize_close(pos, exit_report.avg_price, "abort", exit_report)
            else:
                self._finalize_close(pos, self._last_price(pos), "abort_already_flat")
            # A protection submit may still be in flight inside the OrderManager
            # (wait_for cancelled our await, not the shared submission): sweep
            # again shortly so it cannot land as an orphan reduce-only order.
            task = asyncio.ensure_future(self._late_protection_sweep(pos))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            task.add_done_callback(_consume_task_exception)
        finally:
            self._closing.discard(pos.position_id)

    async def _late_protection_sweep(self, pos: ManagedPosition) -> None:
        await asyncio.sleep(max(1.0, self._config.submit_timeout_s))
        await self._cancel_protections(pos, keep_index=False)

    async def _cancel_protections(self, pos: ManagedPosition, keep_index: bool = False) -> None:
        for cid in (pos.stop_client_order_id, pos.take_profit_client_order_id):
            if not cid:
                continue
            ok = False
            for attempt in range(3):
                try:
                    await self._adapter.cancel(pos.symbol, cid)
                    ok = True
                    break
                except (OrderNotFoundError, InvalidOrderError):
                    ok = True  # never placed / already gone
                    break
                except asyncio.CancelledError:
                    raise
                except ExecutionError:
                    ok = True  # simulator: unknown or already terminal
                    break
                except Exception as exc:  # noqa: BLE001 - network: retry
                    log.warning("protection cancel failed", extra={
                        "client_order_id": cid, "attempt": attempt, "error": repr(exc)})
                    await asyncio.sleep(0.2 * (attempt + 1))
            if ok and not keep_index:
                self._by_protection_cid.pop(cid, None)
            elif not ok:
                METRICS.incr("execution.protection_cancel_failed")
                log.error("protection cancel exhausted; reconciler will sweep",
                          extra={"client_order_id": cid})

    async def _cancel_remaining(self, client_order_id: str, symbol: str) -> None:
        try:
            await self._adapter.cancel(symbol, client_order_id)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - already terminal
            pass

    def _fees_of(self, report: Optional[ExecutionReport]) -> float:
        if report is None:
            return 0.0
        actual = sum(f.fee for f in report.fills if f.fee)
        if actual > 0:
            return actual
        return report.executed_quantity * report.avg_price * self._config.fee_pct

    def _reanchor_protection(self, pos: ManagedPosition, entry_ref_price: Optional[float]) -> None:
        ref = entry_ref_price or 0.0
        if not ref or not pos.entry_price or ref == pos.entry_price:
            return
        sl_dist = abs(ref - pos.stop_loss_price)
        tp_dist = abs(pos.take_profit_price - ref)
        sign = 1.0 if pos.side == "BUY" else -1.0
        pos.stop_loss_price = pos.entry_price - sign * sl_dist
        pos.take_profit_price = pos.entry_price + sign * tp_dist
        if self.filters is not None:
            f = self.filters.get(pos.symbol)
            # stop rounds AWAY from entry (keeps at least the approved distance),
            # TP rounds TOWARD entry (stays reachable).
            pos.stop_loss_price = f.round_price(pos.stop_loss_price, "down" if sign > 0 else "up")
            pos.take_profit_price = f.round_price(pos.take_profit_price, "down" if sign > 0 else "up")

    def _last_price(self, pos: ManagedPosition) -> float:
        getter = getattr(self._adapter, "get_price", None)
        price = getter(pos.symbol) if callable(getter) else None
        return float(price or pos.entry_price)

    def _discard_failed(self, pos: ManagedPosition) -> None:
        """An entry that never filled is not a position: drop its bookkeeping."""
        pos.status = PositionStatus.ABORTED
        pos.close_reason = pos.close_reason or "entry_failed"
        pos.closed_ts_ms = int(time.time() * 1000)
        for cid in (pos.stop_client_order_id, pos.take_profit_client_order_id):
            self._by_protection_cid.pop(cid, None)
        self._by_entry_cid.pop(pos.entry_client_order_id, None)
        self._positions.pop(pos.position_id, None)

    def prune_closed(self, keep: int = 200) -> None:
        """Bound memory in 24/7 sessions: keep only the latest closed positions."""
        closed = [p for p in self._positions.values()
                  if p.status in (PositionStatus.CLOSED, PositionStatus.ABORTED)]
        closed.sort(key=lambda p: p.closed_ts_ms)
        for p in closed[:max(0, len(closed) - keep)]:
            self._positions.pop(p.position_id, None)
            self._by_entry_cid.pop(p.entry_client_order_id, None)
            self._close_seq.pop(p.position_id, None)

    def _finalize_close(self, pos: ManagedPosition, exit_price: float, reason: str,
                        exit_report: Optional[ExecutionReport] = None) -> None:
        if pos.status is PositionStatus.CLOSED:
            return  # idempotent: SL fill + concurrent close must count once
        now_ms = int(time.time() * 1000)
        self._roll_day(now_ms)
        sign = 1.0 if pos.side == "BUY" else -1.0
        realized = (exit_price - pos.entry_price) * sign * pos.quantity
        if exit_report is not None:
            pos.fees += self._fees_of(exit_report)
        else:
            pos.fees += pos.quantity * exit_price * self._config.fee_pct
        net = realized - pos.fees
        pos.realized_pnl = realized
        pos.exit_price = float(exit_price)
        pos.close_reason = reason
        pos.status = PositionStatus.CLOSED
        pos.closed_ts_ms = now_ms
        self._daily_realized_pnl += net
        if net < 0:
            self._consecutive_losses += 1
            self._last_loss_ms = now_ms
        else:
            self._consecutive_losses = 0
        METRICS.incr("execution.positions_closed")
        log.info("position closed", extra={
            "position_id": pos.position_id,
            "symbol": pos.symbol,
            "reason": reason,
            "pnl": round(realized, 6),
            "fees": round(pos.fees, 6),
            "net": round(net, 6),
        })
        # Hygiene: cancel the sibling protection that is still resting on the
        # venue after a SL/TP fill, so no residual reduce-only order can ever
        # pivot the net position on a later price move.
        try:
            task = asyncio.ensure_future(self._cancel_protections(pos))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            task.add_done_callback(_consume_task_exception)
        except RuntimeError:  # no running loop (sync tests)
            pass
        for cb in self._closed_callbacks:
            try:
                cb(pos)
            except Exception:  # noqa: BLE001 - observers must not break execution
                log.exception("position-closed callback failed")

    def _ensure_price(self, symbol: str, entry_ref_price: Optional[float]) -> None:
        if hasattr(self._adapter, "get_price") and self._adapter.get_price(symbol) not in (None, 0):
            return
        if entry_ref_price and entry_ref_price > 0 and hasattr(self._adapter, "set_price"):
            self._adapter.set_price(symbol, entry_ref_price)
            return
        raise ExecutionError(f"no tradable price for {symbol}")

    def _ensure_task_started(self) -> None:
        if self._task is None or self._task.done():
            if self._task is not None and self._task.done() and not self._task.cancelled() \
                    and self._task.exception() is not None:
                log.error("execution consumer died; restarting", exc_info=self._task.exception())
            if self._event_q is None:  # reuse: never leak a dead subscriber queue
                self._event_q = self._adapter.subscribe()
            self._task = asyncio.create_task(self._consume(), name="position-consumer")