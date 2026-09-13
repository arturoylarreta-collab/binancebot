"""Order manager (FASE 6) — idempotent order lifecycle.

Guarantees:
  * every order carries a caller-supplied client_order_id (idempotency key);
  * repeating the same client_order_id NEVER double-submits: a remembered
    terminal outcome is replayed, an in-flight one is awaited;
  * submission retries only recoverable/transient failures with exponential
    backoff, re-using the same client_order_id (safe: the adapter is
    idempotent and raises DuplicateOrderError for repeats it already holds);
  * a timeout cancels the pending order instead of leaving it in limbo;
  * cancel / cancel-and-replace are first-class operations.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType
from crypto_scalper.core.exceptions import (
    DuplicateOrderError,
    ExecutionError,
    InvalidOrderError,
    OrderRejectedError,
    OrderTimeoutError,
    Recoverable,
    Transient,
)
from crypto_scalper.core.models import ExecutionReport, Fill, OrderRequest
from crypto_scalper.execution.base import ExchangeAdapter
from crypto_scalper.monitoring.metrics import METRICS

log = logging.getLogger(__name__)

TERMINAL_STATUSES = frozenset({
    OrderStatus.FILLED.name,
    OrderStatus.CANCELED.name,
    OrderStatus.REJECTED.name,
    OrderStatus.EXPIRED.name,
    OrderStatus.PARTIALLY_FILLED_CANCELED.name,
    OrderStatus.NEW_INSURANCE.name,
    OrderStatus.NEW_ADL.name,
})

_ORDER_TYPE_NAMES = {t.name for t in OrderType}


class OrderManager:
    def __init__(self, adapter: ExchangeAdapter, config: Optional[ExecutionConfig] = None) -> None:
        self._adapter = adapter
        self._config = config or ExecutionConfig()
        self._orders: Dict[str, ExecutionReport] = {}
        self._inflight: Dict[str, "asyncio.Future[ExecutionReport]"] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def adapter(self) -> ExchangeAdapter:
        return self._adapter

    def orders(self) -> Tuple[ExecutionReport, ...]:
        return tuple(self._orders.values())

    def get_order(self, client_order_id: str) -> Optional[ExecutionReport]:
        return self._orders.get(client_order_id)

    def snapshot(self) -> Dict[str, ExecutionReport]:
        return dict(self._orders)

    async def refresh(self) -> None:
        """Sync the local cache with the venue (passive fills / cancellations).

        Orders submitted here are re-fetched from the adapter so the cached
        report reflects fills and status changes that happened on the venue
        without a further submit (e.g. a stop-loss filled by a price tick, or
        a sibling protection cancelled when a position closed).
        """
        for cid in list(self._orders):
            symbol = self._orders[cid].symbol
            report = await self._adapter.get_order(symbol, cid)
            self._orders[cid] = report
        METRICS.incr("execution.orders_refreshed")

    # ── Public API ────────────────────────────────────────────────────────────

    async def submit(
        self,
        request: OrderRequest,
        *,
        wait_fill: bool = False,
        timeout_s: Optional[float] = None,
    ) -> ExecutionReport:
        """Submit an order idempotently (by client_order_id)."""
        self._validate(request)
        client_id = request.client_order_id

        remembered = self._orders.get(client_id)
        if remembered is not None:
            if remembered.is_terminal:
                METRICS.incr("execution.replayed")
                log.info("order replayed", extra={"client_order_id": client_id,
                                                  "status": remembered.status})
                return remembered
            # Non-terminal remembered: fall through to wait on in-flight / poll.

        inflight_fut = self._inflight.get(client_id)
        if inflight_fut is not None:
            return await inflight_fut

        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
        fut: "asyncio.Future[ExecutionReport]" = self._loop.create_future()
        self._inflight[client_id] = fut
        asyncio.ensure_future(self._fulfill(client_id, request, fut, timeout_s, wait_fill))
        return await fut

    async def cancel(self, symbol: str, client_order_id: str) -> ExecutionReport:
        report = await self._adapter.cancel(symbol, client_order_id)
        self._orders[client_order_id] = report
        METRICS.incr("execution.cancelled")
        log.info("order cancelled", extra={"client_order_id": client_order_id,
                                           "status": report.status})
        return report

    async def cancel_and_replace(
        self,
        symbol: str,
        old_client_order_id: str,
        new_request: OrderRequest,
        *,
        timeout_s: Optional[float] = None,
    ) -> ExecutionReport:
        """Cancel the old order (idempotent) then submit the replacement."""
        try:
            await self._adapter.cancel(symbol, old_client_order_id)
        except ExecutionError:
            pass  # already terminal on the venue
        return await self.submit(new_request, wait_fill=False, timeout_s=timeout_s)

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _fulfill(
        self,
        client_id: str,
        request: OrderRequest,
        fut: "asyncio.Future[ExecutionReport]",
        timeout_s: Optional[float],
        wait_fill: bool,
    ) -> None:
        final: Optional[ExecutionReport] = None
        try:
            final = await self._submit_with_retry(request, timeout_s, wait_fill)
            fut.set_result(final)
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
        except BaseException as exc:  # noqa: BLE001
            if not fut.done():
                fut.set_exception(exc)
        finally:
            self._inflight.pop(client_id, None)
            if final is not None:
                self._orders[client_id] = final

    async def _submit_with_retry(
        self,
        request: OrderRequest,
        timeout_s: Optional[float],
        wait_fill: bool,
    ) -> ExecutionReport:
        submit_timeout = timeout_s if timeout_s is not None else self._config.submit_timeout_s
        attempts = self._config.retries + 1
        last_exc: Optional[Exception] = None

        for attempt in range(attempts):
            try:
                report = await asyncio.wait_for(
                    self._adapter.submit(request), timeout=submit_timeout
                )
                return await self._settle(report, request, wait_fill)
            except DuplicateOrderError:
                # Already accepted by the venue but we lost the ack: fetch the
                # existing order — never re-submit (this is the idempotency win).
                METRICS.incr("execution.idempotent_dedupe")
                existing = await self._adapter.get_order(request.symbol, request.client_order_id)
                return await self._settle(existing, request, wait_fill)
            except TimeoutError:
                await self._best_effort_cancel(request.client_order_id, request.symbol)
                raise OrderTimeoutError(
                    f"submit timeout for {request.client_order_id}"
                ) from None
            except OrderTimeoutError:
                # Fill-wait timeout already cancelled the order; retrying would
                # "succeed" against the canceled terminal state. Surface it.
                raise
            except Exception as exc:  # noqa: BLE001
                if not isinstance(exc, (Recoverable, Transient)):
                    raise
                last_exc = exc
                METRICS.incr("execution.retry")
                if attempt < attempts - 1:
                    backoff = min(
                        self._config.backoff_base_s * (2 ** attempt),
                        self._config.backoff_max_s,
                    )
                    log.warning("order submit retry", extra={
                        "client_order_id": request.client_order_id,
                        "attempt": attempt + 1,
                        "backoff_s": round(backoff, 3),
                        "error": type(exc).__name__,
                    })
                    await asyncio.sleep(backoff)
                    continue

        assert last_exc is not None
        raise last_exc

    async def _settle(
        self,
        report: ExecutionReport,
        request: OrderRequest,
        wait_fill: bool,
    ) -> ExecutionReport:
        if report.status == OrderStatus.REJECTED.name:
            raise OrderRejectedError(
                f"order {request.client_order_id} rejected: "
                f"{report.reject_reason or 'rejected_by_exchange'}"
            )
        if wait_fill and not report.is_terminal:
            report = await self._wait_terminal(
                request.symbol, request.client_order_id,
                self._config.fill_timeout_s,
            )
        return report

    async def _wait_terminal(
        self,
        symbol: str,
        client_order_id: str,
        deadline_s: float,
    ) -> ExecutionReport:
        """Poll the venue until a terminal status or the deadline passes.

        Offline adapters are deterministic and never block, so the polling
        cost is negligible here; a live adapter would just return as soon as
        its event stream settles.
        """
        deadline = time.monotonic() + deadline_s
        while True:
            report = await self._adapter.get_order(symbol, client_order_id)
            if report.is_terminal:
                return report
            if time.monotonic() >= deadline:
                await self._best_effort_cancel(client_order_id, symbol)
                raise OrderTimeoutError(
                    f"fill timeout for {client_order_id} "
                    f"(status={report.status}, filled={report.executed_quantity})"
                )
            await asyncio.sleep(0.02)

    async def _best_effort_cancel(
        self,
        client_order_id: str,
        symbol: Optional[str] = None,
    ) -> None:
        try:
            await self._adapter.cancel(symbol or "", client_order_id)
        except Exception:  # noqa: BLE001
            log.warning("best-effort cancel failed",
                        extra={"client_order_id": client_order_id})

    @staticmethod
    def _validate(request: OrderRequest) -> None:
        if not request.symbol:
            raise InvalidOrderError("symbol is required")
        if not request.client_order_id:
            raise InvalidOrderError(
                "client_order_id is required for idempotent execution"
            )
        if request.quantity <= 0:
            raise InvalidOrderError(f"quantity must be > 0, got {request.quantity}")
        if request.order_type not in _ORDER_TYPE_NAMES:
            raise InvalidOrderError(f"unknown order_type {request.order_type!r}")
        if request.order_type == OrderType.LIMIT.name and request.price is None:
            raise InvalidOrderError("LIMIT orders require price")
        if request.order_type in (OrderType.STOP_MARKET.name,
                                  OrderType.TAKE_PROFIT_MARKET.name) \
                and request.stop_price is None:
            raise InvalidOrderError(f"{request.order_type} requires stop_price")