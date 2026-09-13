"""Producer → Queue → Processor → State pipeline for one symbol.

WebSocket producers publish into bounded queues and never wait on the
processor; the processor drains its per-symbol queues and keeps SymbolState
coherent, including order-book resync on gaps.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from crypto_scalper.core.events import EventBus
from crypto_scalper.core.exceptions import ExchangeConnectionError
from crypto_scalper.core.models import AggTrade, DiffDepthEvent
from crypto_scalper.market_data.rest import BinanceFuturesRest
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

log = logging.getLogger(__name__)


class SymbolProcessor:
    """Consumes the per-symbol queues and keeps SymbolState coherent."""

    def __init__(
        self,
        symbol: str,
        state: SymbolState,
        rest: BinanceFuturesRest,
        bus: EventBus,
        metrics: Metrics,
        depth_snapshot_limit: int = 100,
        resync_retries: int = 3,
    ) -> None:
        self.symbol = symbol
        self.state = state
        self._rest = rest
        self._bus = bus
        self._metrics = metrics
        self._depth_snapshot_limit = depth_snapshot_limit
        self._resync_retries = resync_retries
        self._trade_q: Optional[asyncio.Queue] = None
        self._depth_q: Optional[asyncio.Queue] = None
        self._resync_event = asyncio.Event()

    async def start(self) -> None:
        self._trade_q = self._bus.subscribe(f"market.{self.symbol}.trade")
        self._depth_q = self._bus.subscribe(f"market.{self.symbol}.depth")
        await self._resync_now(initial=True)

    async def close(self) -> None:
        if self._trade_q:
            self._bus.unsubscribe(f"market.{self.symbol}.trade", self._trade_q)
        if self._depth_q:
            self._bus.unsubscribe(f"market.{self.symbol}.depth", self._depth_q)

    async def run(self, stop_event: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._consume_trades(stop_event)),
            asyncio.create_task(self._consume_depth(stop_event)),
            asyncio.create_task(self._resync_loop(stop_event)),
        ]
        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consume_trades(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            item = await self._trade_q.get()
            if isinstance(item, AggTrade):
                self.state.on_trade(item)

    async def _consume_depth(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            item = await self._depth_q.get()
            if isinstance(item, DiffDepthEvent):
                self.state.on_depth(item)
            # Any gap (or pre-snapshot buffering with a lost snapshot) means
            # the book needs a REST resync. Do not hammer while suspended.
            if self.state.orderbook.sync_required and not self.state.suspended:
                self._resync_event.set()

    async def _resync_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self._resync_event.wait()
            await self._resync_now()
            self._resync_event.clear()
            await asyncio.sleep(0.05)

    async def _resync_now(self, initial: bool = False) -> None:
        for attempt in range(self._resync_retries):
            try:
                data = await self._rest.depth(self.symbol, self._depth_snapshot_limit)
                ok = self.state.orderbook.apply_snapshot(
                    last_update_id=int(data["lastUpdateId"]),
                    bids=data["bids"],
                    asks=data["asks"],
                    ts_ms=self.state.utc_now_ms(),
                )
                self._metrics.set_gauge(f"ob.{self.symbol}.synced", 1 if ok else 0)
                if ok:
                    self.state.suspended = False
                    log.info("orderbook synced", extra={"symbol": self.symbol})
                    return
                log.warning(
                    "orderbook snapshot replay gap",
                    extra={"symbol": self.symbol, "attempt": attempt},
                )
            except ExchangeConnectionError as exc:
                log.warning(
                    "orderbook resync failed",
                    extra={"symbol": self.symbol, "attempt": attempt, "error": str(exc)},
                )
                await asyncio.sleep(0.5 * (attempt + 1))

        self.state.suspended = True
        self._metrics.set_gauge(f"ob.{self.symbol}.synced", 0)
        log.error("orderbook resync exhausted", extra={"symbol": self.symbol})