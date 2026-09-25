"""WebSocket producer for Binance USDⓈ-M Futures combined streams.

- groups symbols into a configurable number of physical connections
- normalizes raw frames into typed domain events (AggTrade, DiffDepthEvent)
- never blocks the receive loop on downstream analysis (publish_nowait)
- reconnects with exponential backoff + jitter, responds to ping/pong
- a stale receive timeout forces reconnect (recv_timeout_s)

Order-book continuity across reconnect is handled by the per-symbol
OrderBook gap detection, which triggers a REST resync.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from crypto_scalper.config.settings import WebSocketConfig
from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.events import (
    DepthEvent,
    EventBus,
    LifecycleEvent,
    LIFECYCLE,
    SYMBOL_DEPTH,
    SYMBOL_TRADE,
    TradeEvent,
    QueueFullError,
)
from crypto_scalper.core.models import AggTrade, DiffDepthEvent
from crypto_scalper.monitoring.metrics import Metrics

log = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self, config: WebSocketConfig, bus: EventBus, metrics: Metrics) -> None:
        self._config = config
        # "partial" = <sym>@depth20@100ms (full top-20 book per message, no REST);
        # "diff" = <sym>@depth (incremental, needs REST snapshots + sync).
        self.depth_mode = getattr(config, "depth_mode", "diff")
        self._bus = bus
        self._metrics = metrics
        self._url = config.url
        self._tasks: List[asyncio.Task] = []
        self.last_message_mono: float = 0.0
        self.connected: int = 0

    def build_streams(self, symbols: List[str]) -> List[str]:
        """aggTrade + depth stream (diff or partial top-20) per symbol."""
        depth = "depth20@100ms" if self.depth_mode == "partial" else "depth"
        streams = []
        for sym in symbols:
            s = sym.lower()
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@{depth}")
        return streams

    def _batches(self, streams: List[str]) -> List[List[str]]:
        size = self._config.batch_size
        return [streams[i : i + size] for i in range(0, len(streams), size)]

    def routed_batches(self, symbols: List[str]) -> List[Tuple[str, List[str]]]:
        """(url, streams) per physical connection.

        Binance routes USD-M market streams by category: ``@aggTrade`` is only
        delivered on ``/market`` and ``@depth`` on ``/public`` (a legacy
        ``/stream`` connection silently receives depth only).
        """
        base = self._url.rstrip("/")
        for suffix in ("/stream", "/ws"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
        for suffix in ("/public", "/market"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
        trades = [s for s in self.build_streams(symbols) if s.endswith("@aggTrade")]
        depth = [s for s in self.build_streams(symbols) if not s.endswith("@aggTrade")]
        out: List[Tuple[str, List[str]]] = []
        for batch in self._batches(trades):
            out.append((f"{base}/market/stream", batch))
        for batch in self._batches(depth):
            out.append((f"{base}/public/stream", batch))
        return out

    async def run(self, symbols: List[str], stop_event: asyncio.Event) -> None:
        """Keep all connection tasks alive in parallel until stop is set."""
        batches = self.routed_batches(symbols)
        n_streams = sum(len(b) for _, b in batches)
        log.info("ws manager starting", extra={"streams": n_streams, "connections": len(batches)})
        self._metrics.set_gauge("ws.streams", n_streams)
        self._metrics.set_gauge("ws.connections.target", len(batches))

        tasks = [
            asyncio.create_task(self._run_connection(batch, idx, url))
            for idx, (url, batch) in enumerate(batches)
        ]
        self._tasks = tasks
        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_connection(self, streams: List[str], idx: int, url: str = "") -> None:
        attempt = 0
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            try:
                await self._connect_and_read(streams, idx, url or self._url)
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001 - reconnector must survive any failure
                log.warning(
                    "ws connection dropped",
                    extra={"conn": idx, "attempt": attempt, "error": repr(exc)},
                )
            finally:
                self._metrics.incr("ws.disconnects")

            self._metrics.incr("ws.reconnects")
            # A connection that lived long enough was healthy: Binance closes
            # every stream after 24h, which must not escalate the backoff.
            if loop.time() - started > 60.0:
                attempt = 0
            delay = self._backoff_delay(attempt)
            attempt += 1
            self._metrics.set_gauge("ws.backoff_s", delay)
            log.info("ws reconnecting", extra={"conn": idx, "attempt": attempt, "delay_s": delay})
            await asyncio.sleep(delay)

    def _backoff_delay(self, attempt: int) -> float:
        cfg = self._config
        base = cfg.reconnect_base_s * (cfg.reconnect_factor ** min(attempt, 10))
        delay = min(base, cfg.reconnect_max_s)
        jitter = random.uniform(0, min(0.5, delay * 0.2))
        return delay + jitter

    async def _connect_and_read(self, streams: List[str], idx: int, base_url: str = "") -> None:
        query = "/".join(streams)
        url = f"{base_url or self._url}?streams={query}"
        timeout = aiohttp.ClientTimeout(total=self._config.conn_timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(url, autoping=True) as ws:
                log.info("ws connected", extra={"conn": idx})
                self._metrics.incr("ws.connections.active")
                self.connected += 1
                await self._bus.publish(
                    LifecycleEvent(topic=LIFECYCLE, kind="ws.connected", detail={"conn": idx})
                )
                try:
                    await self._read_loop(ws, idx)
                finally:
                    self.connected -= 1
                    self._metrics.decr("ws.connections.active")

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse, idx: int) -> None:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=self._config.recv_timeout_s)
            except asyncio.TimeoutError:
                # No inbound traffic within the window: the feed is stale.
                raise ExchangeStalled("no data for recv_timeout_s")
            except aiohttp.ClientError as exc:
                self._metrics.incr("ws.errors")
                raise exc

            if msg.type == aiohttp.WSMsgType.TEXT:
                self.last_message_mono = asyncio.get_running_loop().time()
                await self._handle_frame(msg.data, idx, ws)
            elif msg.type == aiohttp.WSMsgType.PING:
                await ws.pong(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise ExchangeStalled(f"websocket closed: {msg.type}")

    async def _handle_frame(self, text: str, idx: int, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            frame = _parse_json(text)
        except ValueError as exc:
            self._metrics.incr("ws.malformed")
            log.warning("malformed ws frame", extra={"conn": idx, "error": str(exc)})
            return

        if not isinstance(frame, dict):
            return
        payload = frame.get("data") if "data" in frame else frame
        if not isinstance(payload, dict):
            return

        event_type = payload.get("e")
        symbol = str(payload.get("s", "")).upper()
        if not symbol or not event_type:
            return

        if event_type == "aggTrade":
            event = _parse_agg_trade(payload)
            if event is not None:
                self._metrics.incr("ws.events.trade")
                await self._publish_checked(
                    TradeEvent(topic=SYMBOL_TRADE.format(symbol), trade=event)
                )
        elif event_type == "depthUpdate":
            event = _parse_depth_update(payload, is_snapshot=self.depth_mode == "partial")
            if event is not None:
                self._metrics.incr("ws.events.depth")
                await self._publish_checked(
                    DepthEvent(topic=SYMBOL_DEPTH.format(symbol), diff=event)
                )
        elif event_type == "bookTicker":
            self._metrics.incr("ws.events.bookticker")
        else:
            self._metrics.incr("ws.events.unhandled")

    async def _publish_checked(self, event) -> None:
        try:
            self._bus.publish_nowait(event)
        except QueueFullError:
            # A bounded consumer queue overflowed: signal a resync for that
            # symbol instead of blocking the WebSocket receive loop.
            self._metrics.incr("ws.queue_overflow")
            log.warning("ws queue overflow; event dropped", extra={"topic": event.topic})
            # Depth gaps are caught by the pu-continuity check, which triggers a
            # REST resync; dropped trades only degrade one feature window.


class ExchangeStalled(Exception):
    pass


def _parse_json(text: str) -> Any:
    import json
    return json.loads(text)


def _parse_agg_trade(payload: Dict[str, Any]) -> Optional[AggTrade]:
    try:
        return AggTrade(
            symbol=str(payload["s"]).upper(),
            event_time_ms=int(payload["T"]),
            trade_id=int(payload["a"]),
            price=float(payload["p"]),
            quantity=float(payload["q"]),
            aggressor=AggressorSide.SELL if payload.get("m") else AggressorSide.BUY,
            ingest_mono_ms=_mono_ms(),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _parse_depth_update(payload: Dict[str, Any], is_snapshot: bool = False) -> Optional[DiffDepthEvent]:
    try:
        bids = tuple((float(p), float(q)) for p, q in payload["b"])
        asks = tuple((float(p), float(q)) for p, q in payload["a"])
        return DiffDepthEvent(
            symbol=str(payload["s"]).upper(),
            event_time_ms=int(payload["E"]),
            first_update_id=int(payload["U"]),
            final_update_id=int(payload["u"]),
            previous_final_update_id=int(payload["pu"]),
            bids=bids,
            asks=asks,
            ingest_mono_ms=_mono_ms(),
            is_snapshot=is_snapshot,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _mono_ms() -> int:
    import time
    return int(time.monotonic() * 1000)