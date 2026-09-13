"""Exchange adapter contract (FASE 6).

ExchangeAdapter is the thin seam between the execution layer and any venue.
Only OrderRequest (in) and ExecutionReport (out) cross it, so strategies,
risk and position management never see Binance-specific payloads.

The simulated adapter (execution/simulated.py) implements this contract
offline; a Binance futures adapter will implement the same interface in a
later phase without touching the OrderManager / PositionManager above it.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Tuple

from crypto_scalper.core.exceptions import DuplicateOrderError
from crypto_scalper.core.models import ExecutionReport, OrderRequest


class ExchangeAdapter(ABC):
    """Contract every exchange implementation must honor."""

    async def start(self) -> None:  # pragma: no cover - trivially no-op
        """Open connections/sessions. Idempotent."""

    async def close(self) -> None:  # pragma: no cover - trivially no-op
        """Close connections/sessions. Idempotent."""

    @abstractmethod
    async def submit(self, request: OrderRequest) -> ExecutionReport:
        """Submit an order and return its initial report.

        Must raise DuplicateOrderError when the same client_order_id was
        already accepted (idempotency safety net below the OrderManager).
        """

    @abstractmethod
    async def cancel(self, symbol: str, client_order_id: str) -> ExecutionReport:
        """Cancel a resting order by client_order_id."""

    @abstractmethod
    async def get_order(self, symbol: str, client_order_id: str) -> ExecutionReport:
        """Latest report for an order."""

    @abstractmethod
    async def open_positions(self) -> Tuple[dict, ...]:
        """Venue-reported positions (for reconciliation), e.g.
        ({"symbol": "BTCUSDT", "side": "LONG", "quantity": 5.0}, ...)
        """

    @abstractmethod
    async def open_orders(self, symbol: Optional[str] = None) -> Tuple[ExecutionReport, ...]:
        """All resting (non-terminal) orders, optionally per symbol."""

    @abstractmethod
    def subscribe(self) -> asyncio.Queue:
        """Bounded queue receiving an ExecutionReport copy on every change.

        Consumers call subscribe() once and drain with `await q.get()`.
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable adapter identity (used in logs/verifier)."""