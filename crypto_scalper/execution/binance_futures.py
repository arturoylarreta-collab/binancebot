"""Binance USD-M Futures execution adapter (testnet/demo or live).

Implements the same ``ExchangeAdapter`` contract as the simulator, so the
OrderManager / PositionManager / Reconciler above it are unchanged:

  * MARKET / LIMIT go to ``/fapi/v1/order`` (``newOrderRespType=RESULT`` so a
    market entry reports its fill synchronously);
  * STOP_MARKET / TAKE_PROFIT_MARKET go to the Algo Service
    (``/fapi/v1/algoOrder``, mandatory since 2025-12-09; the legacy endpoint
    answers -4120). Protections trigger on ``MARK_PRICE`` so a thin testnet
    book cannot wick them;
  * state changes are detected twice: the user-data stream (fast path) and a
    REST poll of every non-terminal tracked order (safety net). Both feed the
    same diff → ``ExecutionReport`` emission, so a dropped stream never means
    a missed stop-loss fill.

One-way position mode is required (reduce-only semantics); ``start()``
switches the account to it, sets leverage per symbol and loads the exchange
filters into the shared ``FilterRegistry``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType
from crypto_scalper.core.exceptions import (
    ConfigurationError,
    DuplicateOrderError,
    ExchangeError,
    ExecutionError,
    OrderNotFoundError,
    OrderRejectedError,
)
from crypto_scalper.core.models import ExecutionReport, Fill, OrderRequest
from crypto_scalper.execution.base import ExchangeAdapter
from crypto_scalper.execution.binance_client import BinanceFuturesClient
from crypto_scalper.execution.filters import FilterRegistry, format_decimal, parse_symbol_filters
from crypto_scalper.monitoring.metrics import METRICS

log = logging.getLogger(__name__)

_ALGO_TYPES = {OrderType.STOP_MARKET.name, OrderType.TAKE_PROFIT_MARKET.name}
_TERMINAL = {
    OrderStatus.FILLED.name, OrderStatus.CANCELED.name, OrderStatus.REJECTED.name,
    OrderStatus.EXPIRED.name, OrderStatus.PARTIALLY_FILLED_CANCELED.name,
    OrderStatus.NEW_INSURANCE.name, OrderStatus.NEW_ADL.name,
}


@dataclass
class _Tracked:
    symbol: str
    kind: str                      # "order" | "algo"
    request: OrderRequest
    report: Optional[ExecutionReport] = None
    actual_order_id: str = ""      # algo → triggered order id
    fills: Dict[str, Fill] = field(default_factory=dict)   # trade_id → Fill


class BinanceFuturesAdapter(ExchangeAdapter):
    def __init__(
        self,
        client: BinanceFuturesClient,
        *,
        ws_base_url: str,
        config: Optional[ExecutionConfig] = None,
        filters: Optional[FilterRegistry] = None,
        symbols: Optional[List[str]] = None,
        leverage: int = 1,
        price_source: Optional[Callable[[str], Optional[float]]] = None,
        poll_interval_s: float = 2.0,
        label: str = "binance-testnet",
    ) -> None:
        self._client = client
        self._ws_base = ws_base_url.rstrip("/")
        for suffix in ("/stream", "/ws"):
            if self._ws_base.endswith(suffix):
                self._ws_base = self._ws_base[: -len(suffix)]
        self._config = config or ExecutionConfig()
        self.filters = filters or FilterRegistry()
        self._symbols = [s.upper() for s in (symbols or [])]
        self._leverage = max(1, int(leverage))
        self._price_source = price_source
        self._poll_interval_s = poll_interval_s
        self._label = label

        self._tracked: Dict[str, _Tracked] = {}
        self._by_order_id: Dict[str, str] = {}   # venue orderId → client id
        self._subscribers: List[asyncio.Queue] = []
        self._tasks: List[asyncio.Task] = []
        self._started = False
        self._listen_key: Optional[str] = None
        self._marks: Dict[str, float] = {}
        self._account: Dict[str, float] = {"wallet": 0.0, "unrealized": 0.0, "available": 0.0}
        self._positions: Dict[str, Tuple[str, float, float]] = {}  # symbol → (side, qty, entry)
        self.stream_connected = False
        self.last_stream_event_ms = 0

    # ── lifecycle ───────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return self._label

    @property
    def client(self) -> BinanceFuturesClient:
        return self._client

    async def start(self) -> None:
        if self._started:
            return
        await self._client.start()
        await self._load_filters()
        await self._ensure_one_way_mode()
        for sym in self._symbols:
            await self._set_leverage(sym)
        await self.refresh_account()
        self._started = True
        self._tasks = [
            asyncio.create_task(self._poll_loop(), name="binance-poll"),
            asyncio.create_task(self._user_stream_loop(), name="binance-user-stream"),
            asyncio.create_task(self._account_loop(), name="binance-account"),
        ]
        log.info("binance adapter started", extra={
            "venue": self._label, "base": self._client.base_url,
            "symbols": ",".join(self._symbols), "leverage": self._leverage,
            "wallet": round(self._account["wallet"], 2)})

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        if self._listen_key:
            try:
                await self._client.request("DELETE", "/fapi/v1/listenKey")
            except Exception:  # noqa: BLE001 - best effort
                pass
        await self._client.close()
        self._started = False

    # ── contract ────────────────────────────────────────────────────────────

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._config.event_queue_size)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    def get_price(self, symbol: str) -> Optional[float]:
        if self._price_source is not None:
            p = self._price_source(symbol)
            if p:
                return float(p)
        return self._marks.get(symbol)

    def account_snapshot(self) -> Dict[str, float]:
        return dict(self._account)

    async def submit(self, request: OrderRequest) -> ExecutionReport:
        cid = request.client_order_id or ""
        if len(cid) > 36:
            raise ExecutionError(f"client_order_id too long for Binance: {cid}")
        existing = self._tracked.get(cid)
        if existing is not None and existing.report is not None and not existing.report.is_terminal:
            raise DuplicateOrderError(f"{cid} already working on venue")

        f = self.filters.get(request.symbol)
        qty = f.floor_qty(request.quantity, market=request.order_type != OrderType.LIMIT.name)
        if qty <= 0:
            raise OrderRejectedError(f"{cid}: quantity {request.quantity} below lot step")
        qty_s = format_decimal(qty, f.market_step_size or f.step_size) \
            if request.order_type != OrderType.LIMIT.name else format_decimal(qty, f.step_size)

        if request.order_type in _ALGO_TYPES:
            trigger = f.round_price(float(request.stop_price or 0.0))
            params = {
                "algoType": "CONDITIONAL",
                "symbol": request.symbol,
                "side": request.side,
                "type": request.order_type,
                "quantity": qty_s,
                "triggerPrice": format_decimal(trigger, f.tick_size),
                "workingType": "MARK_PRICE",
                "reduceOnly": bool(request.reduce_only),
                "clientAlgoId": cid,
                "newOrderRespType": "RESULT",
            }
            tracked = _Tracked(request.symbol, "algo", request)
            self._tracked[cid] = tracked
            try:
                data = await self._client.request("POST", "/fapi/v1/algoOrder", params, signed=True)
            except BaseException:
                self._forget_if_unconfirmed(cid)
                raise
            report = self._algo_report(tracked, data)
        else:
            params = {
                "symbol": request.symbol,
                "side": request.side,
                "type": request.order_type,
                "quantity": qty_s,
                "newClientOrderId": cid,
                "newOrderRespType": "RESULT",
            }
            if request.reduce_only:
                params["reduceOnly"] = True
            if request.order_type == OrderType.LIMIT.name:
                params["price"] = format_decimal(f.round_price(float(request.price)), f.tick_size)
                params["timeInForce"] = request.time_in_force or "GTC"
            tracked = _Tracked(request.symbol, "order", request)
            self._tracked[cid] = tracked
            try:
                data = await self._client.request("POST", "/fapi/v1/order", params, signed=True)
            except BaseException:
                self._forget_if_unconfirmed(cid)
                raise
            report = self._order_report(tracked, data)
        METRICS.incr(f"binance.submit.{request.order_type.lower()}")
        self._update(cid, report)
        return report

    async def cancel(self, symbol: str, client_order_id: str) -> ExecutionReport:
        tracked = self._tracked.get(client_order_id)
        kind = tracked.kind if tracked else ("algo" if client_order_id.startswith(("SL-", "TP-")) else "order")
        sym = symbol or (tracked.symbol if tracked else "")
        try:
            if kind == "algo":
                await self._client.request("DELETE", "/fapi/v1/algoOrder",
                                           {"clientAlgoId": client_order_id}, signed=True)
            else:
                await self._client.request("DELETE", "/fapi/v1/order",
                                           {"symbol": sym, "origClientOrderId": client_order_id},
                                           signed=True)
        except ExchangeError:
            raise  # network/rate-limit: caller retries
        except ExecutionError:
            # Unknown / already terminal: report the venue truth instead.
            report = await self.get_order(sym, client_order_id)
            if report.is_terminal:
                return report
            raise
        METRICS.incr("binance.cancel")
        return await self.get_order(sym, client_order_id)

    async def get_order(self, symbol: str, client_order_id: str) -> ExecutionReport:
        tracked = self._tracked.get(client_order_id)
        if tracked is None:
            kind = "algo" if client_order_id.startswith(("SL-", "TP-")) else "order"
            tracked = _Tracked(symbol, kind, OrderRequest(
                symbol=symbol, side="BUY", order_type=OrderType.MARKET.name,
                quantity=0.0, client_order_id=client_order_id))
        report = await self._fetch(tracked, client_order_id)
        if client_order_id in self._tracked:
            self._update(client_order_id, report)
        return report

    async def open_positions(self) -> Tuple[dict, ...]:
        await self._refresh_positions()
        return tuple(
            {"symbol": s, "side": side, "quantity": qty}
            for s, (side, qty, _entry) in self._positions.items() if qty > 0
        )

    async def open_orders(self, symbol: Optional[str] = None) -> Tuple[ExecutionReport, ...]:
        symbols = [symbol] if symbol else (self._symbols or [None])
        out: List[ExecutionReport] = []
        for sym in symbols:
            params = {"symbol": sym} if sym else {}
            for o in await self._client.request("GET", "/fapi/v1/openOrders", params, signed=True):
                cid = str(o.get("clientOrderId", ""))
                tracked = self._tracked.get(cid) or _Tracked(o["symbol"], "order", _placeholder(o))
                out.append(self._order_report(tracked, o))
            algo = await self._client.request("GET", "/fapi/v1/openAlgoOrders", params, signed=True)
            rows = algo.get("orders", algo) if isinstance(algo, dict) else algo
            for a in rows or []:
                cid = str(a.get("clientAlgoId", ""))
                tracked = self._tracked.get(cid) or _Tracked(a["symbol"], "algo", _placeholder(a))
                rep = self._algo_report(tracked, a)
                if not rep.is_terminal:
                    out.append(rep)
        return tuple(out)

    # ── account ─────────────────────────────────────────────────────────────

    async def refresh_account(self) -> Dict[str, float]:
        data = await self._first_ok([("GET", "/fapi/v3/account"), ("GET", "/fapi/v2/account")])
        self._account = {
            "wallet": float(data.get("totalWalletBalance", 0.0)),
            "unrealized": float(data.get("totalUnrealizedProfit", 0.0)),
            "available": float(data.get("availableBalance", 0.0)),
            "margin": float(data.get("totalMaintMargin", 0.0)),
        }
        METRICS.set_gauge("binance.wallet", self._account["wallet"])
        return dict(self._account)

    async def _refresh_positions(self) -> None:
        rows = await self._first_ok([("GET", "/fapi/v3/positionRisk"), ("GET", "/fapi/v2/positionRisk")])
        positions: Dict[str, Tuple[str, float, float]] = {}
        for r in rows:
            amt = float(r.get("positionAmt", 0.0))
            sym = str(r.get("symbol", ""))
            if r.get("markPrice"):
                self._marks[sym] = float(r["markPrice"])
            if abs(amt) > 0:
                positions[sym] = ("LONG" if amt > 0 else "SHORT", abs(amt),
                                  float(r.get("entryPrice", 0.0)))
        self._positions = positions

    async def _first_ok(self, candidates):
        last: Optional[Exception] = None
        for method, path in candidates:
            try:
                return await self._client.request(method, path, signed=True)
            except (OrderRejectedError, ExecutionError) as exc:  # 404-ish on older venues
                last = exc
        assert last is not None
        raise last

    # ── setup ───────────────────────────────────────────────────────────────

    async def _load_filters(self) -> None:
        info = await self._client.request("GET", "/fapi/v1/exchangeInfo")
        wanted = set(self._symbols)
        parsed = [parse_symbol_filters(s) for s in info.get("symbols", [])
                  if not wanted or s.get("symbol") in wanted]
        self.filters.update(parsed)
        missing = wanted - {f.symbol for f in parsed}
        if missing:
            raise ConfigurationError(f"symbols not listed on {self._label}: {sorted(missing)}")

    async def _ensure_one_way_mode(self) -> None:
        data = await self._client.request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if data.get("dualSidePosition") in (True, "true"):
            try:
                await self._client.request("POST", "/fapi/v1/positionSide/dual",
                                           {"dualSidePosition": False}, signed=True)
                log.warning("switched account to one-way position mode")
            except ExecutionError as exc:
                raise ConfigurationError(
                    "account is in hedge mode and cannot be switched (close positions first)"
                ) from exc

    async def _set_leverage(self, symbol: str) -> None:
        try:
            await self._client.request("POST", "/fapi/v1/leverage",
                                       {"symbol": symbol, "leverage": self._leverage}, signed=True)
        except ExecutionError as exc:
            log.warning("leverage change failed", extra={"symbol": symbol, "error": str(exc)})

    # ── report mapping ──────────────────────────────────────────────────────

    def _order_report(self, tracked: _Tracked, o: Dict[str, Any]) -> ExecutionReport:
        status = _map_status(str(o.get("status", "NEW")), float(o.get("executedQty", 0.0)))
        cid = str(o.get("clientOrderId", tracked.request.client_order_id or ""))
        order_id = str(o.get("orderId", ""))
        if order_id:
            self._by_order_id[order_id] = cid
        executed = float(o.get("executedQty", 0.0))
        avg = float(o.get("avgPrice", 0.0) or 0.0)
        return ExecutionReport(
            order_id=order_id,
            client_order_id=cid,
            symbol=str(o.get("symbol", tracked.symbol)),
            side=str(o.get("side", tracked.request.side)),
            order_type=str(o.get("type", o.get("origType", tracked.request.order_type))),
            status=status,
            original_quantity=float(o.get("origQty", tracked.request.quantity)),
            executed_quantity=executed,
            avg_price=avg,
            fills=self._fills_for(tracked, cid, order_id, executed, avg, int(o.get("updateTime", 0))),
            reject_reason="",
            ts_ms=int(o.get("updateTime", o.get("time", 0)) or 0),
        )

    def _algo_report(self, tracked: _Tracked, a: Dict[str, Any],
                     actual: Optional[Dict[str, Any]] = None) -> ExecutionReport:
        algo_status = str(a.get("algoStatus", "NEW")).upper()
        actual_id = str(a.get("actualOrderId", "") or "")
        if actual_id:
            tracked.actual_order_id = actual_id
            self._by_order_id[actual_id] = str(a.get("clientAlgoId", tracked.request.client_order_id))
        executed = float(actual.get("executedQty", 0.0)) if actual else 0.0
        avg = float(actual.get("avgPrice", 0.0) or 0.0) if actual else float(a.get("actualPrice", 0.0) or 0.0)
        if actual:
            status = _map_status(str(actual.get("status", "NEW")), executed)
        elif algo_status in ("NEW", "TRIGGERING", "TRIGGERED"):
            status = OrderStatus.NEW.name   # still protecting (or executing)
        elif algo_status == "CANCELED":
            status = OrderStatus.CANCELED.name
        elif algo_status == "REJECTED":
            status = OrderStatus.REJECTED.name
        elif algo_status == "FINISHED":
            # finished without a readable triggered order: trust actualQty/Price
            executed = float(a.get("actualQty", 0.0) or 0.0) if avg > 0 else 0.0
            status = OrderStatus.FILLED.name if executed > 0 else OrderStatus.EXPIRED.name
        else:
            status = OrderStatus.EXPIRED.name
        cid = str(a.get("clientAlgoId", tracked.request.client_order_id or ""))
        return ExecutionReport(
            order_id=str(a.get("algoId", "")),
            client_order_id=cid,
            symbol=str(a.get("symbol", tracked.symbol)),
            side=str(a.get("side", tracked.request.side)),
            order_type=str(a.get("orderType", a.get("type", tracked.request.order_type))),
            status=status,
            original_quantity=float(a.get("quantity", tracked.request.quantity) or 0.0),
            executed_quantity=executed,
            avg_price=avg,
            fills=self._fills_for(tracked, cid, actual_id, executed, avg,
                                  int(a.get("updateTime", 0) or 0)),
            reject_reason="" if status != OrderStatus.REJECTED.name else algo_status,
            ts_ms=int(a.get("updateTime", a.get("createTime", 0)) or 0),
        )

    def _fills_for(self, tracked: _Tracked, cid: str, order_id: str, executed: float,
                   avg: float, ts_ms: int) -> Tuple[Fill, ...]:
        if tracked.fills:
            return tuple(tracked.fills.values())
        if executed <= 0:
            return ()
        return (Fill(symbol=tracked.symbol, client_order_id=cid, side=tracked.request.side,
                     quantity=executed, price=avg, ts_ms=ts_ms, order_id=order_id,
                     trade_id=f"agg-{order_id}"),)

    async def _fetch(self, tracked: _Tracked, cid: str) -> ExecutionReport:
        if tracked.kind == "algo":
            a = await self._client.request("GET", "/fapi/v1/algoOrder",
                                           {"clientAlgoId": cid}, signed=True)
            actual = None
            actual_id = str(a.get("actualOrderId", "") or "")
            if actual_id:
                try:
                    actual = await self._client.request(
                        "GET", "/fapi/v1/order",
                        {"symbol": a.get("symbol", tracked.symbol), "orderId": actual_id},
                        signed=True)
                except OrderNotFoundError:
                    actual = None
            return self._algo_report(tracked, a, actual)
        o = await self._client.request("GET", "/fapi/v1/order",
                                       {"symbol": tracked.symbol, "origClientOrderId": cid},
                                       signed=True)
        return self._order_report(tracked, o)

    def _forget_if_unconfirmed(self, cid: str) -> None:
        tracked = self._tracked.get(cid)
        if tracked is not None and tracked.report is None:
            self._tracked.pop(cid, None)

    # ── change detection / emission ─────────────────────────────────────────

    def _update(self, cid: str, report: ExecutionReport) -> None:
        tracked = self._tracked.get(cid)
        if tracked is None:
            return
        prev = tracked.report
        tracked.report = report
        if prev is None or (prev.status, prev.executed_quantity) != (report.status, report.executed_quantity):
            self._emit(report)

    def _emit(self, report: ExecutionReport) -> None:
        for q in list(self._subscribers):
            if q.full():
                try:
                    q.get_nowait()  # drop oldest: the poller re-syncs state anyway
                    METRICS.incr("binance.events_dropped")
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(report)

    async def _poll_loop(self) -> None:
        """Safety net: re-fetch every non-terminal tracked order on a cadence."""
        while True:
            await asyncio.sleep(self._poll_interval_s)
            for cid, tracked in list(self._tracked.items()):
                if tracked.report is not None and tracked.report.is_terminal:
                    continue
                try:
                    self._update(cid, await self._fetch(tracked, cid))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    METRICS.incr("binance.poll_errors")
                    log.debug("order poll failed", extra={"client_order_id": cid, "error": repr(exc)})
            self._prune()

    def _prune(self, keep: int = 300) -> None:
        terminal = [c for c, t in self._tracked.items() if t.report is not None and t.report.is_terminal]
        for cid in terminal[:max(0, len(terminal) - keep)]:
            t = self._tracked.pop(cid)
            self._by_order_id.pop(t.report.order_id if t.report else "", None)
            self._by_order_id.pop(t.actual_order_id, None)

    async def _account_loop(self) -> None:
        while True:
            await asyncio.sleep(15.0)
            try:
                await self.refresh_account()
                await self._refresh_positions()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("account refresh failed", extra={"error": repr(exc)})

    # ── user-data stream ────────────────────────────────────────────────────

    async def _user_stream_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                data = await self._client.request("POST", "/fapi/v1/listenKey")
                self._listen_key = data["listenKey"]
                await self._run_user_stream(self._listen_key)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("user stream dropped", extra={"error": repr(exc), "retry_s": backoff})
            self.stream_connected = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _run_user_stream(self, listen_key: str) -> None:
        urls = [f"{self._ws_base}/private/ws?listenKey={listen_key}", f"{self._ws_base}/ws/{listen_key}"]
        last_exc: Optional[Exception] = None
        for url in urls:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=60, autoping=True) as ws:
                        self.stream_connected = True
                        log.info("user stream connected", extra={"path": url.split("?")[0].rsplit("/", 1)[0]})
                        keepalive = asyncio.create_task(self._keepalive())
                        try:
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    if self._on_user_event(json.loads(msg.data)) == "expired":
                                        return
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            keepalive.cancel()
                        return
            except (aiohttp.WSServerHandshakeError, aiohttp.ClientError) as exc:
                last_exc = exc
                continue
        if last_exc is not None:
            raise last_exc

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(30 * 60)
            try:
                await self._client.request("PUT", "/fapi/v1/listenKey")
            except Exception as exc:  # noqa: BLE001
                log.warning("listenKey keepalive failed", extra={"error": repr(exc)})

    def _on_user_event(self, event: Dict[str, Any]) -> Optional[str]:
        payload = event.get("data", event)
        etype = payload.get("e")
        self.last_stream_event_ms = int(time.time() * 1000)
        METRICS.incr(f"binance.stream.{etype}")
        if etype == "listenKeyExpired":
            return "expired"
        if etype == "ORDER_TRADE_UPDATE":
            o = payload.get("o", {})
            cid = str(o.get("c", ""))
            order_id = str(o.get("i", ""))
            owner = cid if cid in self._tracked else self._by_order_id.get(order_id)
            if owner and owner in self._tracked and o.get("x") == "TRADE" and float(o.get("l", 0)) > 0:
                tracked = self._tracked[owner]
                trade_id = str(o.get("t", ""))
                tracked.fills[trade_id] = Fill(
                    symbol=str(o.get("s")), client_order_id=owner, side=str(o.get("S")),
                    quantity=float(o.get("l", 0.0)), price=float(o.get("L", 0.0)),
                    ts_ms=int(o.get("T", 0)), order_id=order_id,
                    fee=float(o.get("n", 0.0) or 0.0), fee_asset=str(o.get("N") or "USDT"),
                    trade_id=trade_id)
            if owner:
                self._schedule_refresh(owner)
        elif etype == "ALGO_UPDATE":
            o = payload.get("o", {})
            cid = str(o.get("caid", ""))
            if o.get("ai"):
                self._by_order_id[str(o["ai"])] = cid
            if cid in self._tracked:
                self._schedule_refresh(cid)
        elif etype == "ACCOUNT_UPDATE":
            a = payload.get("a", {})
            for b in a.get("B", []):
                if b.get("a") == "USDT":
                    self._account["wallet"] = float(b.get("wb", self._account["wallet"]))
            for p in a.get("P", []):
                amt = float(p.get("pa", 0.0))
                sym = str(p.get("s"))
                if abs(amt) > 0:
                    self._positions[sym] = ("LONG" if amt > 0 else "SHORT", abs(amt), float(p.get("ep", 0.0)))
                else:
                    self._positions.pop(sym, None)
        return None

    def _schedule_refresh(self, cid: str) -> None:
        tracked = self._tracked.get(cid)
        if tracked is None:
            return

        async def _refresh() -> None:
            try:
                self._update(cid, await self._fetch(tracked, cid))
            except Exception as exc:  # noqa: BLE001 - poller retries
                log.debug("event refresh failed", extra={"client_order_id": cid, "error": repr(exc)})

        task = asyncio.create_task(_refresh())
        self._tasks.append(task)
        task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)


def _map_status(status: str, executed: float) -> str:
    s = status.upper()
    if s == "EXPIRED_IN_MATCH":
        return OrderStatus.EXPIRED.name
    if s in ("CANCELED", "EXPIRED") and executed > 0:
        return OrderStatus.PARTIALLY_FILLED_CANCELED.name
    if s in OrderStatus.__members__:
        return s
    return OrderStatus.NEW.name


def _placeholder(o: Dict[str, Any]) -> OrderRequest:
    return OrderRequest(
        symbol=str(o.get("symbol", "")),
        side=str(o.get("side", "BUY")),
        order_type=str(o.get("type", o.get("orderType", OrderType.MARKET.name))),
        quantity=float(o.get("origQty", o.get("quantity", 0.0)) or 0.0),
        client_order_id=str(o.get("clientOrderId", o.get("clientAlgoId", ""))),
    )


async def preflight(settings) -> Optional[str]:
    """Verify testnet credentials before committing the engine to the venue.

    Returns None when the account is reachable and the keys can trade, or a
    human-readable reason otherwise (the caller then stays on paper instead of
    crash-looping on a typo'd key).
    """
    vc = settings.venue
    client = BinanceFuturesClient(vc.testnet_rest_url, vc.api_key, vc.api_secret,
                                  recv_window_ms=vc.recv_window_ms)
    try:
        await client.start()
        acct = None
        for path in ("/fapi/v3/account", "/fapi/v2/account"):
            try:
                acct = await client.request("GET", path, signed=True)
                break
            except OrderRejectedError:
                continue
        if not acct:
            return "account endpoint unavailable"
        if not acct.get("canTrade", True):
            return "API key has no futures trading permission"
        return None
    except Exception as exc:  # noqa: BLE001 - reported to the operator verbatim
        return f"{type(exc).__name__}: {exc}"[:240]
    finally:
        await client.close()
