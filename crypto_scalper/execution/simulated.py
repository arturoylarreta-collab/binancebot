"""Simulated exchange adapter (FASE 6) — fully offline, deterministic.

Implements ExchangeAdapter without any network. State is a last price per
symbol plus the resting-book; price moves drive LIMIT / STOP_MARKET /
TAKE_PROFIT_MARKET fills (Binance futures trigger semantics):

    STOP_MARKET        BUY triggers when price >= stop_price
                       SELL triggers when price <= stop_price
    TAKE_PROFIT_MARKET BUY triggers when price <= stop_price
                       SELL triggers when price >= stop_price
    LIMIT              BUY fills when price <= limit
                       SELL fills when price >= limit

Fills slip by `slippage_pct`: BUY slips up, SELL slips down. reduce_only
fills are clamped to the venue net position (isolated-margin semantics) so
a stop cannot open a new position on top of the one it is closing.

Partial fills are supported via schedule_partial() so the OrderManager and
PositionManager can be exercised against non-trivial executions.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType, Side
from crypto_scalper.core.exceptions import DuplicateOrderError, ExecutionError
from crypto_scalper.core.models import ExecutionReport, Fill, OrderRequest
from crypto_scalper.execution.base import ExchangeAdapter

log = logging.getLogger(__name__)

_TERMINAL = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
    OrderStatus.PARTIALLY_FILLED_CANCELED,
    OrderStatus.NEW_INSURANCE,
    OrderStatus.NEW_ADL,
}


@dataclass
class _SimOrder:
    request: OrderRequest
    order_id: str
    created_ms: int
    status: str = OrderStatus.NEW.name
    filled_qty: float = 0.0
    avg_price: float = 0.0
    fills: List[Fill] = field(default_factory=list)
    reject_reason: str = ""
    partial_plan: Deque[float] = field(default_factory=deque)

    @property
    def remaining(self) -> float:
        return max(0.0, self.request.quantity - self.filled_qty)

    @property
    def is_resting(self) -> bool:
        return self.status in (OrderStatus.NEW.name, OrderStatus.PARTIALLY_FILLED.name)


class SimulatedExecutionAdapter(ExchangeAdapter):
    """In-memory exchange: predictable fills, price-driven triggers."""

    def __init__(self, config: Optional[ExecutionConfig] = None) -> None:
        self._config = config or ExecutionConfig()
        self._last_price: Dict[str, float] = {}
        self._orders: Dict[str, _SimOrder] = {}
        self._net_positions: Dict[str, float] = {}
        self._subscribers: List[asyncio.Queue] = []
        self._pending_partial: Dict[str, Deque[float]] = {}
        self._counter = 0

    @property
    def name(self) -> str:
        return "simulated"

    # ── Price simulation ──────────────────────────────────────────────────────

    def set_price(self, symbol: str, price: float) -> Tuple[ExecutionReport, ...]:
        """Set the last price for `symbol` and trigger crossing resting orders.

        Returns the reports of every order that changed because of the move.
        """
        if price <= 0:
            raise ExecutionError(f"invalid simulated price {price} for {symbol}")
        self._last_price[symbol] = price
        changed: List[ExecutionReport] = []
        triggered = [o for o in self._orders.values()
                     if o.request.symbol == symbol and o.is_resting
                     and self._should_trigger(o, price)]
        for order in triggered:
            changed.append(self._fill_order(order, price))
        return tuple(changed)

    def get_price(self, symbol: str) -> Optional[float]:
        return self._last_price.get(symbol)

    def schedule_partial(self, client_order_id: str, amounts: List[float]) -> None:
        """Pre-register an absolute-qty fill schedule for an order.

        May be called before or after submit(). The first chunk is applied on
        submit/trigger; the rest is applied by subsequent complete_partial() /
        set_price() passes. Sum of amounts may be less than the full quantity
        (the order stays partially filled).
        """
        for amount in amounts:
            if amount <= 0:
                raise ExecutionError(f"partial plan must be > 0, got {amount}")
        self._pending_partial[client_order_id] = deque(amounts)
        order = self._orders.get(client_order_id)
        if order is not None:
            order.partial_plan = self._pending_partial.pop(client_order_id)

    def complete_partial(self, client_order_id: str) -> ExecutionReport:
        """Force whatever remains of a partially filled order to fill now."""
        order = self._orders.get(client_order_id)
        if order is None:
            raise KeyError(f"unknown client_order_id {client_order_id!r}")
        if not order.is_resting:
            raise ExecutionError(f"order {client_order_id} is not resting")
        report = self._report(order)
        if order.remaining <= 0:
            return report
        before = order.filled_qty
        self._apply_fill(order, order.remaining, self._last_price.get(order.request.symbol, 0.0))
        order.partial_plan.clear()
        if order.filled_qty != before:
            report = self._report(order)
            self._emit(report)
        return report

    # ── ExchangeAdapter ───────────────────────────────────────────────────────

    async def submit(self, request: OrderRequest) -> ExecutionReport:
        if request.client_order_id in self._orders:
            raise DuplicateOrderError(
                f"client_order_id {request.client_order_id!r} already accepted"
            )
        symbol = request.symbol
        price = self._last_price.get(symbol)
        if price is None or price <= 0:
            raise ExecutionError(f"no last price available for {symbol}")

        order = _SimOrder(
            request=request,
            order_id=self._next_order_id(),
            created_ms=request.requested_ts_ms or int(time.time() * 1000),
        )
        if request.client_order_id in self._pending_partial:
            order.partial_plan = self._pending_partial.pop(request.client_order_id)
        otype = request.order_type

        if otype == OrderType.MARKET.name:
            order = self._initial_fill(order, price)
        elif otype == OrderType.LIMIT.name and self._crossed_limit(request, price):
            order = self._initial_fill(order, price)
        else:
            # Resting limit, stop-market or take-profit-market.
            order.status = OrderStatus.NEW.name

        if order.status not in (OrderStatus.NEW.name, OrderStatus.PARTIALLY_FILLED.name) \
                and order.status != OrderStatus.FILLED.name:
            order.reject_reason = "simulated_reject"

        self._orders[request.client_order_id] = order
        report = self._report(order)
        self._emit(report)
        return report

    async def cancel(self, symbol: str, client_order_id: str) -> ExecutionReport:
        order = self._orders.get(client_order_id)
        if order is None:
            raise ExecutionError(f"unknown order {client_order_id!r}")
        if not order.is_resting:
            return self._report(order)  # already final; idempotent no-op
        order.status = (
            OrderStatus.PARTIALLY_FILLED_CANCELED.name
            if order.filled_qty > 0
            else OrderStatus.CANCELED.name
        )
        report = self._report(order)
        self._emit(report)
        return report

    async def get_order(self, symbol: str, client_order_id: str) -> ExecutionReport:
        order = self._orders.get(client_order_id)
        if order is None:
            raise ExecutionError(f"unknown order {client_order_id!r}")
        return self._report(order)

    async def open_positions(self) -> Tuple[dict, ...]:
        result: List[dict] = []
        for symbol, net in sorted(self._net_positions.items()):
            if abs(net) < 1e-12:
                continue
            result.append({
                "symbol": symbol,
                "side": "LONG" if net > 0 else "SHORT",
                "quantity": abs(net),
            })
        return tuple(result)

    async def open_orders(self, symbol: Optional[str] = None) -> Tuple[ExecutionReport, ...]:
        return tuple(
            self._report(o)
            for o in self._orders.values()
            if o.is_resting and (symbol is None or o.request.symbol == symbol)
        )

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._config.event_queue_size)
        self._subscribers.append(q)
        return q

    # ── Internals ─────────────────────────────────────────────────────────────

    def _next_order_id(self) -> str:
        self._counter += 1
        return f"sim-{self._counter:08d}"

    @staticmethod
    def _crossed_limit(request: OrderRequest, price: float) -> bool:
        if request.side == Side.BUY.name:
            return price <= request.price
        return price >= request.price

    @staticmethod
    def _should_trigger(order: _SimOrder, price: float) -> bool:
        r = order.request
        otype = r.order_type
        if otype == OrderType.LIMIT.name:
            return SimulatedExecutionAdapter._crossed_limit(r, price)
        if otype == OrderType.STOP_MARKET.name:
            if r.side == Side.BUY.name:
                return price >= r.stop_price
            return price <= r.stop_price
        if otype == OrderType.TAKE_PROFIT_MARKET.name:
            if r.side == Side.BUY.name:
                return price <= r.stop_price
            return price >= r.stop_price
        return False

    def _initial_fill(self, order: _SimOrder, price: float) -> _SimOrder:
        plan = order.partial_plan
        if plan:
            chunk = plan.popleft()
        else:
            chunk = order.remaining
        self._apply_fill(order, chunk, price)
        return order

    def _fill_order(self, order: _SimOrder, price: float) -> ExecutionReport:
        plan = order.partial_plan
        chunk = plan.popleft() if plan else order.remaining
        report = self._report(order)
        if chunk <= 0:
            return report
        before = order.filled_qty
        self._apply_fill(order, chunk, price)
        if order.filled_qty != before:
            report = self._report(order)
            self._emit(report)
        return report

    def _apply_fill(self, order: _SimOrder, chunk: float, ref_price: float) -> None:
        r = order.request
        if r.reduce_only:
            max_fill = self._reduceable(r.symbol, r.side)
            chunk = min(chunk, max_fill)
        chunk = min(chunk, order.remaining)
        if chunk <= 0:
            return

        slip = self._config.slippage_pct
        if r.side == Side.BUY.name:
            fill_price = ref_price * (1.0 + slip)
            sign = 1.0
        else:
            fill_price = ref_price * (1.0 - slip)
            sign = -1.0

        order.filled_qty += chunk
        self._net_positions[r.symbol] = self._net_positions.get(r.symbol, 0.0) + sign * chunk
        order.fills.append(Fill(
            symbol=r.symbol,
            client_order_id=r.client_order_id,
            side=r.side,
            quantity=chunk,
            price=round(fill_price, 12),
            ts_ms=int(time.time() * 1000),
            order_id=order.order_id,
        ))
        if order.filled_qty >= r.quantity - 1e-12:
            order.status = OrderStatus.FILLED.name
        else:
            order.status = OrderStatus.PARTIALLY_FILLED.name
        total = order.filled_qty
        weighted = sum(f.quantity * f.price for f in order.fills)
        order.avg_price = weighted / total if total > 0 else 0.0

    def _reduceable(self, symbol: str, side: str) -> float:
        net = self._net_positions.get(symbol, 0.0)
        if side == Side.SELL.name:
            return max(0.0, net)      # close long exposure
        return max(0.0, -net)          # close short exposure

    def _report(self, order: _SimOrder) -> ExecutionReport:
        return ExecutionReport(
            order_id=order.order_id,
            client_order_id=order.request.client_order_id,
            symbol=order.request.symbol,
            side=order.request.side,
            order_type=order.request.order_type,
            status=order.status,
            original_quantity=order.request.quantity,
            executed_quantity=order.filled_qty,
            avg_price=order.avg_price,
            fills=tuple(order.fills),
            reject_reason=order.reject_reason,
            ts_ms=order.created_ms,
        )

    def _emit(self, report: ExecutionReport) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(report)
            except asyncio.QueueFull:
                raise ExecutionError(
                    f"execution event queue full (client_order_id={report.client_order_id})"
                )