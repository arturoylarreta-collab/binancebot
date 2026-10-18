"""Producer → Queue → Processor → State pipeline for one symbol.

WebSocket producers publish into bounded queues and never wait on the
processor; the processor drains its per-symbol queues and keeps SymbolState
coherent, including order-book resync on gaps.

Resilience contract (24/7 unattended):
  * any exception while applying one event is logged and counted, never fatal
  * a resync first clears the book so diffs buffer while the REST snapshot is
    in flight (Binance protocol); it retries with capped exponential backoff
    forever instead of suspending the symbol permanently
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from crypto_scalper.core.events import DepthEvent, EventBus, TradeEvent
from crypto_scalper.core.models import AggTrade, DiffDepthEvent
from crypto_scalper.market_data.rest import BinanceFuturesRest
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

log = logging.getLogger(__name__)

_RESYNC_BACKOFF_MAX_S = 30.0


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
        depth_mode: str = "diff",
    ) -> None:
        self._depth_mode = depth_mode
        self.symbol = symbol
        self.state = state
        self._rest = rest
        self._bus = bus
        self._metrics = metrics
        self._depth_snapshot_limit = depth_snapshot_limit
        self._resync_retries = max(1, resync_retries)
        self._trade_q: Optional[asyncio.Queue] = None
        self._depth_q: Optional[asyncio.Queue] = None
        self._resync_event = asyncio.Event()

    async def start(self) -> None:
        """Subscribe to the bus. The first snapshot is taken by :meth:`run`
        once the WebSocket is streaming, so buffered diffs bridge the gap."""
        if self._trade_q is None:
            self._trade_q = self._bus.subscribe(f"market.{self.symbol}.trade")
            self._depth_q = self._bus.subscribe(f"market.{self.symbol}.depth")
        if self._depth_mode != "partial":
            self._resync_event.set()   # partial streams carry the whole book

    async def close(self) -> None:
        if self._trade_q:
            self._bus.unsubscribe(f"market.{self.symbol}.trade", self._trade_q)
        if self._depth_q:
            self._bus.unsubscribe(f"market.{self.symbol}.depth", self._depth_q)

    async def run(self, stop_event: asyncio.Event) -> None:
        if self._trade_q is None:
            await self.start()
        tasks = [
            asyncio.create_task(self._consume_trades(stop_event), name=f"trades-{self.symbol}"),
            asyncio.create_task(self._consume_depth(stop_event), name=f"depth-{self.symbol}"),
            asyncio.create_task(self._resync_loop(stop_event), name=f"resync-{self.symbol}"),
        ]
        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consume_trades(self, stop_event: asyncio.Event) -> None:
        assert self._trade_q is not None
        while not stop_event.is_set():
            item = await self._trade_q.get()
            trade = item.trade if isinstance(item, TradeEvent) else item
            if not isinstance(trade, AggTrade):
                continue
            try:
                self.state.on_trade(trade)
            except Exception:  # noqa: BLE001 - one bad event must not kill the feed
                self._metrics.incr("processor.trade_errors")
                log.exception("trade apply failed", extra={"symbol": self.symbol})

    async def _consume_depth(self, stop_event: asyncio.Event) -> None:
        assert self._depth_q is not None
        while not stop_event.is_set():
            item = await self._depth_q.get()
            diff = item.diff if isinstance(item, DepthEvent) else item
            if not isinstance(diff, DiffDepthEvent):
                continue
            try:
                self.state.on_depth(diff)
            except Exception:  # noqa: BLE001
                self._metrics.incr("processor.depth_errors")
                log.exception("depth apply failed", extra={"symbol": self.symbol})
                self.state.orderbook.sync_required = True
            if (self._depth_mode != "partial" and self.state.orderbook.has_snapshot
                    and self.state.orderbook.sync_required):
                self._resync_event.set()

    async def _resync_loop(self, stop_event: asyncio.Event) -> None:
        failures = 0
        while not stop_event.is_set():
            await self._resync_event.wait()
            self._resync_event.clear()
            ok = False
            try:
                ok = await self._resync_now()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - keep retrying forever
                log.warning("orderbook resync error",
                            extra={"symbol": self.symbol, "error": repr(exc)})
            if ok:
                failures = 0
                await asyncio.sleep(0.05)
                continue
            failures += 1
            delay = min(_RESYNC_BACKOFF_MAX_S, 0.5 * (2 ** min(failures, 6)))
            self._metrics.set_gauge(f"ob.{self.symbol}.resync_backoff_s", delay)
            await asyncio.sleep(delay)
            self._resync_event.set()

    async def _resync_now(self, initial: bool = False) -> bool:
        book = self.state.orderbook
        for attempt in range(self._resync_retries):
            book.begin_resync()  # buffer diffs while the snapshot is in flight
            try:
                data = await self._rest.depth(self.symbol, self._depth_snapshot_limit)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - network/rate-limit/payload
                log.warning(
                    "orderbook snapshot failed",
                    extra={"symbol": self.symbol, "attempt": attempt, "error": repr(exc)},
                )
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            ok = book.apply_snapshot(
                last_update_id=int(data["lastUpdateId"]),
                bids=data["bids"],
                asks=data["asks"],
                ts_ms=self.state.utc_now_ms(),
            )
            self._metrics.set_gauge(f"ob.{self.symbol}.synced", 1 if ok else 0)
            if ok:
                self.state.suspended = False
                self._metrics.incr("ob.resync_ok")
                log.info("orderbook synced",
                         extra={"symbol": self.symbol, "resyncs": book.resyncs})
                return True
            log.warning("orderbook snapshot replay gap",
                        extra={"symbol": self.symbol, "attempt": attempt})

        self.state.suspended = True
        self._metrics.set_gauge(f"ob.{self.symbol}.synced", 0)
        self._metrics.incr("ob.resync_exhausted")
        log.error("orderbook resync exhausted; backing off", extra={"symbol": self.symbol})
        return False
