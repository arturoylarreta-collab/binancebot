"""Bar-driven venue for backtesting (FASE 8) — deterministic, offline.

Implementa `ExchangeAdapter` pero consume **kline closes** en vez de ticks:
`process_bar(symbol, candle)` evalúa todas las órdenes resting contra el rango
high/low de la barra con semántica de trigger de Binance Futures:

    STOP_MARKET        BUY triggers when price >= stop_price
                       SELL triggers when price <= stop_price
    TAKE_PROFIT_MARKET BUY triggers when price <= stop_price
                       SELL triggers when price >= stop_price
    LIMIT              BUY fills when low <= price
                       SELL fills when high >= price

Reglas de realismo (documentadas, conservadoras):

  * Stop-first dentro de la misma barra: si high/low cruzan SL y TP a la vez,
    se procesa primero el SL (se asume el movimiento adverso primero). El
    clamp `reduce_only` refuerza que un TP no abra posición nueva.
  * Fills de MARKET disparados (SL/TP) se ejecutan al precio de trigger +
    slippage (BUY arriba, SELL abajo). Los LIMIT se ejecutan a su precio
    (sin slippage: el limit ya es la peor ejecución legítima).
  * `entry_fill_fraction` < 1.0 modela entries con ejecución parcial
    determinista (la orden queda PARTIALLY_FILLED y el PositionManager la
    cancela y protege solo lo ejecutado).
  * `set_clock(ms)` / `process_bar` fijan el `clock_ms`: toda timestamp del
    backtest usa tiempo HISTÓRICO (no wall-clock) → determinista y auditables.
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
from crypto_scalper.core.models import Candle, ExecutionReport, Fill, OrderRequest
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

# Prioridad de resolución dentro de una barra (stop-first).
_PRIORITY = {OrderType.STOP_MARKET.name: 0, OrderType.TAKE_PROFIT_MARKET.name: 1, OrderType.LIMIT.name: 2}


@dataclass
class _HistOrder:
    request: OrderRequest
    order_id: str
    created_ms: int
    status: str = OrderStatus.NEW.name
    filled_qty: float = 0.0
    avg_price: float = 0.0
    fills: List[Fill] = field(default_factory=list)
    reject_reason: str = ""

    @property
    def remaining(self) -> float:
        return max(0.0, self.request.quantity - self.filled_qty)

    @property
    def is_resting(self) -> bool:
        return self.status in (OrderStatus.NEW.name, OrderStatus.PARTIALLY_FILLED.name)


class BarVenue(ExchangeAdapter):
    """In-memory exchange that only knows bar ranges (historical replay)."""

    def __init__(
        self,
        config: Optional[ExecutionConfig] = None,
        *,
        entry_fill_fraction: float = 1.0,
        clock_ms: int = 0,
    ) -> None:
        self._config = config or ExecutionConfig()
        self._last_price: Dict[str, float] = {}
        self._orders: Dict[str, _HistOrder] = {}
        self._net_positions: Dict[str, float] = {}
        self._subscribers: List[asyncio.Queue] = []
        self._counter = 0
        self._entry_fill_fraction = entry_fill_fraction
        self._clock_ms = clock_ms

    @property
    def name(self) -> str:
        return "bar_venue"

    # ── Historic clock ────────────────────────────────────────────────────────

    def set_clock(self, ts_ms: int) -> None:
        self._clock_ms = int(ts_ms)

    def _now_ms(self) -> int:
        return self._clock_ms if self._clock_ms > 0 else int(time.time() * 1000)

    # ── Bar processing ────────────────────────────────────────────────────────

    def process_bar(self, symbol: str, candle: Candle) -> Tuple[ExecutionReport, ...]:
        """Evalúa órdenes resting contra el rango de la kline.

        Procesa stop-first (SL antes que TP/LIMIT) y cierra con el close como
        último precio conocido. Devuelve los reports de lo que cambió.
        """
        self._last_price[symbol] = candle.open
        self.set_clock(candle.ts_ms + candle.interval_s * 1000 - 1)
        changed: List[ExecutionReport] = []
        resting = [
            o for o in self._orders.values()
            if o.request.symbol == symbol and o.is_resting
        ]
        resting.sort(key=lambda o: (_PRIORITY.get(o.request.order_type, 9), o.request.client_order_id))
        for order in resting:
            fill_price = self._bar_trigger_price(order, candle)
            if fill_price is not None:
                report = self._fill_order(order, fill_price)
                if report is not None:
                    changed.append(report)
        self._last_price[symbol] = candle.close
        return tuple(changed)

    def set_price(self, symbol: str, price: float) -> Tuple[ExecutionReport, ...]:
        """Push un tick (usado para next_open: entrada al open de la barra)."""
        if price <= 0:
            raise ExecutionError(f"invalid bar price {price} for {symbol}")
        self._last_price[symbol] = price
        changed: List[ExecutionReport] = []
        for client_id in sorted(self._orders):
            order = self._orders[client_id]
            if order.request.symbol != symbol or not order.is_resting:
                continue
            if order.request.order_type == OrderType.LIMIT.name and self._crossed_limit(order.request, price):
                report = self._fill_order(order, order.request.price)
                if report is not None:
                    changed.append(report)
        return tuple(changed)

    def get_price(self, symbol: str) -> Optional[float]:
        return self._last_price.get(symbol)

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

        order = _HistOrder(
            request=request,
            order_id=self._next_order_id(),
            created_ms=request.requested_ts_ms or self._now_ms(),
        )
        otype = request.order_type
        if otype == OrderType.MARKET.name:
            self._fill_order(order, price)
        elif otype == OrderType.LIMIT.name and self._crossed_limit(request, price):
            self._fill_order(order, request.price)
        else:
            order.status = OrderStatus.NEW.name

        if order.status not in (OrderStatus.NEW.name, OrderStatus.PARTIALLY_FILLED.name) \
                and order.status != OrderStatus.FILLED.name:
            order.reject_reason = "bar_venue_reject"

        self._orders[request.client_order_id] = order
        report = self._report(order)
        self._emit(report)
        return report

    async def cancel(self, symbol: str, client_order_id: str) -> ExecutionReport:
        order = self._orders.get(client_order_id)
        if order is None:
            raise ExecutionError(f"unknown order {client_order_id!r}")
        if not order.is_resting:
            return self._report(order)
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
        return f"bt-{self._counter:08d}"

    @staticmethod
    def _crossed_limit(request: OrderRequest, price: float) -> bool:
        if request.side == Side.BUY.name:
            return price <= request.price
        return price >= request.price

    @classmethod
    def _bar_trigger_price(cls, order: _HistOrder, candle: Candle) -> Optional[float]:
        """Precio de fill (trigger) si la barra cruza la orden; None si no."""
        request = order.request
        otype = request.order_type
        if otype == OrderType.LIMIT.name:
            if request.side == Side.BUY.name and candle.low <= request.price:
                return request.price
            if request.side == Side.SELL.name and candle.high >= request.price:
                return request.price
            return None
        if otype == OrderType.STOP_MARKET.name:
            if request.side == Side.BUY.name and candle.high >= request.stop_price:
                return request.stop_price
            if request.side == Side.SELL.name and candle.low <= request.stop_price:
                return request.stop_price
            return None
        if otype == OrderType.TAKE_PROFIT_MARKET.name:
            if request.side == Side.BUY.name and candle.low <= request.stop_price:
                return request.stop_price
            if request.side == Side.SELL.name and candle.high >= request.stop_price:
                return request.stop_price
            return None
        return None

    def _fill_order(self, order: _HistOrder, ref_price: float) -> Optional[ExecutionReport]:
        chunk = order.remaining
        if order.request.reduce_only:
            chunk = min(chunk, self._reduceable(order.request.symbol, order.request.side))
        if chunk <= 0:
            return None  # cerrado por clamp reduce_only: no-op
        before = order.filled_qty
        self._apply_fill(order, chunk, ref_price)
        if order.filled_qty == before:
            return None
        report = self._report(order)
        self._emit(report)
        return report

    def _apply_fill(self, order: _HistOrder, chunk: float, ref_price: float) -> None:
        request = order.request
        if not request.reduce_only and order.filled_qty == 0 and self._entry_fill_fraction < 1.0:
            chunk = min(chunk, max(0.0, request.quantity * self._entry_fill_fraction))
        chunk = min(chunk, order.remaining)
        if chunk <= 0:
            return

        if request.order_type == OrderType.LIMIT.name:
            fill_price = request.price  # el limit ya es la peor ejecución legítima
        else:
            slip = self._config.slippage_pct
            if request.side == Side.BUY.name:
                fill_price = ref_price * (1.0 + slip)
            else:
                fill_price = ref_price * (1.0 - slip)
        fill_price = round(fill_price, 12)

        sign = 1.0 if request.side == Side.BUY.name else -1.0
        order.filled_qty += chunk
        self._net_positions[request.symbol] = self._net_positions.get(request.symbol, 0.0) + sign * chunk
        order.fills.append(Fill(
            symbol=request.symbol,
            client_order_id=request.client_order_id,
            side=request.side,
            quantity=chunk,
            price=fill_price,
            ts_ms=self._now_ms(),
            order_id=order.order_id,
        ))
        if order.filled_qty >= request.quantity - 1e-12:
            order.status = OrderStatus.FILLED.name
        else:
            order.status = OrderStatus.PARTIALLY_FILLED.name
        total = order.filled_qty
        weighted = sum(f.quantity * f.price for f in order.fills)
        order.avg_price = weighted / total if total > 0 else 0.0

    def _reduceable(self, symbol: str, side: str) -> float:
        net = self._net_positions.get(symbol, 0.0)
        if side == Side.SELL.name:
            return max(0.0, net)
        return max(0.0, -net)

    def _report(self, order: _HistOrder) -> ExecutionReport:
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