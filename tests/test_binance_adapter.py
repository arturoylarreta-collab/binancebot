"""BinanceFuturesAdapter against an in-memory fake of the USD-M REST API.

The fake reproduces the endpoints and payload shapes the adapter relies on
(orders, Algo Service conditional orders, positions, account), so the whole
stack — OrderManager → PositionManager → adapter — is exercised offline:
entry fill, SL/TP on /fapi/v1/algoOrder, a stop trigger, and the close.
"""

from __future__ import annotations

import asyncio
import itertools

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.core.enums import OrderStatus, OrderType, PositionStatus, RiskVerdict, SignalType
from crypto_scalper.core.exceptions import (
    AlreadyFlatError,
    DuplicateOrderError,
    ExchangeAuthenticationError,
    ExchangeConnectionError,
    ExchangeRateLimitError,
    OrderNotFoundError,
    OrderRejectedError,
)
from crypto_scalper.core.models import OrderRequest, RiskDecision, Signal
from crypto_scalper.execution.binance_client import _map_error
from crypto_scalper.execution.binance_futures import BinanceFuturesAdapter
from crypto_scalper.execution.filters import FilterRegistry
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager

SYMBOL = "BTCUSDT"


class FakeBinance:
    """Minimal stateful fake of the endpoints the adapter uses."""

    def __init__(self, price: float = 50_000.0) -> None:
        self.price = price
        self.ids = itertools.count(1000)
        self.orders = {}        # clientOrderId → order dict
        self.algos = {}         # clientAlgoId → algo dict
        self.position = 0.0
        self.wallet = 5_000.0
        self.calls = []
        self.base_url = "https://fake"

    # client API --------------------------------------------------------
    async def start(self):
        pass

    async def close(self):
        pass

    async def request(self, method, path, params=None, *, signed=False):
        params = dict(params or {})
        self.calls.append((method, path, params))
        handler = getattr(self, f"_{method.lower()}_{path.strip('/').replace('/', '_')}", None)
        if handler is None:
            raise OrderRejectedError(f"unhandled {method} {path}")
        return handler(params)

    # endpoints ---------------------------------------------------------
    def _get_fapi_v1_exchangeInfo(self, p):
        return {"symbols": [{"symbol": SYMBOL, "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "maxQty": "120"},
            {"filterType": "MIN_NOTIONAL", "notional": "100"},
        ]}]}

    def _get_fapi_v1_positionSide_dual(self, p):
        return {"dualSidePosition": False}

    def _post_fapi_v1_leverage(self, p):
        return {"leverage": int(p["leverage"]), "symbol": p["symbol"]}

    def _get_fapi_v3_account(self, p):
        return {"totalWalletBalance": str(self.wallet), "totalUnrealizedProfit": "0",
                "availableBalance": str(self.wallet), "totalMaintMargin": "0"}

    def _get_fapi_v3_positionRisk(self, p):
        return [{"symbol": SYMBOL, "positionAmt": str(self.position),
                 "entryPrice": str(self.price), "markPrice": str(self.price)}]

    def _post_fapi_v1_listenKey(self, p):
        raise ExchangeConnectionError("no stream in tests")

    def _post_fapi_v1_order(self, p):
        cid = p["newClientOrderId"]
        if cid in self.orders and self.orders[cid]["status"] == "NEW":
            raise DuplicateOrderError("-4116")
        qty = float(p["quantity"])
        signed_qty = qty if p["side"] == "BUY" else -qty
        if p.get("reduceOnly") in ("true", True):
            if self.position == 0 or (self.position > 0) == (signed_qty > 0):
                raise AlreadyFlatError("-2022 ReduceOnly Order is rejected")
        self.position += signed_qty
        o = {"orderId": next(self.ids), "clientOrderId": cid, "symbol": p["symbol"],
             "side": p["side"], "type": p["type"], "status": "FILLED", "origQty": p["quantity"],
             "executedQty": p["quantity"], "avgPrice": str(self.price), "updateTime": 1}
        self.orders[cid] = o
        return o

    def _get_fapi_v1_order(self, p):
        if "orderId" in p:
            for o in self.orders.values():
                if str(o["orderId"]) == str(p["orderId"]):
                    return o
            raise OrderNotFoundError("-2013")
        o = self.orders.get(p["origClientOrderId"])
        if o is None:
            raise OrderNotFoundError("-2013 Order does not exist")
        return o

    def _delete_fapi_v1_order(self, p):
        o = self.orders.get(p["origClientOrderId"])
        if o is None or o["status"] != "NEW":
            raise OrderNotFoundError("-2011 Unknown order sent")
        o["status"] = "CANCELED"
        return o

    def _post_fapi_v1_algoOrder(self, p):
        assert p["algoType"] == "CONDITIONAL" and p["workingType"] == "MARK_PRICE"
        cid = p["clientAlgoId"]
        a = {"algoId": next(self.ids), "clientAlgoId": cid, "algoType": "CONDITIONAL",
             "orderType": p["type"], "symbol": p["symbol"], "side": p["side"],
             "quantity": p["quantity"], "triggerPrice": p["triggerPrice"], "algoStatus": "NEW",
             "actualOrderId": "", "actualPrice": "0", "reduceOnly": p["reduceOnly"],
             "createTime": 1, "updateTime": 1}
        self.algos[cid] = a
        return a

    def _get_fapi_v1_algoOrder(self, p):
        a = self.algos.get(p["clientAlgoId"])
        if a is None:
            raise OrderNotFoundError("-2013")
        return a

    def _delete_fapi_v1_algoOrder(self, p):
        a = self.algos.get(p["clientAlgoId"])
        if a is None or a["algoStatus"] != "NEW":
            raise OrderNotFoundError("-2011")
        a["algoStatus"] = "CANCELED"
        return a

    def _get_fapi_v1_openOrders(self, p):
        return [o for o in self.orders.values() if o["status"] == "NEW"]

    def _get_fapi_v1_openAlgoOrders(self, p):
        return [a for a in self.algos.values() if a["algoStatus"] == "NEW"]

    # test helpers ------------------------------------------------------
    def trigger(self, cid: str, fill_price: float) -> None:
        """Simulate the matching engine triggering + filling a conditional order."""
        a = self.algos[cid]
        qty = float(a["quantity"])
        self.position += qty if a["side"] == "BUY" else -qty
        oid = next(self.ids)
        self.orders[f"auto-{oid}"] = {
            "orderId": oid, "clientOrderId": f"auto-{oid}", "symbol": a["symbol"], "side": a["side"],
            "type": "MARKET", "status": "FILLED", "origQty": a["quantity"], "executedQty": a["quantity"],
            "avgPrice": str(fill_price), "updateTime": 2}
        a.update(algoStatus="FINISHED", actualOrderId=str(oid), actualPrice=str(fill_price))


def _adapter(fake: FakeBinance, poll_s: float = 0.02) -> BinanceFuturesAdapter:
    return BinanceFuturesAdapter(fake, ws_base_url="wss://fake/stream",
                                 config=ExecutionConfig(), filters=FilterRegistry(),
                                 symbols=[SYMBOL], price_source=lambda s: fake.price,
                                 poll_interval_s=poll_s)


async def _wait(pred, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


class TestClientErrorMapping:
    @pytest.mark.parametrize("status,code,exc", [
        (400, -2011, OrderNotFoundError), (400, -2013, OrderNotFoundError),
        (400, -4116, DuplicateOrderError), (400, -2022, AlreadyFlatError),
        (401, -2015, ExchangeAuthenticationError), (429, -1003, ExchangeRateLimitError),
        (400, -2019, OrderRejectedError), (503, 0, ExchangeConnectionError),
    ])
    def test_codes(self, status, code, exc):
        assert isinstance(_map_error(status, code, "x", "POST /fapi/v1/order"), exc)


class TestAdapter:
    async def test_start_loads_filters_and_account(self):
        fake = FakeBinance()
        ad = _adapter(fake)
        await ad.start()
        try:
            assert SYMBOL in ad.filters and ad.filters.get(SYMBOL).tick_size == 0.1
            assert ad.account_snapshot()["wallet"] == 5_000.0
            assert ("POST", "/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": 1}) in fake.calls
        finally:
            await ad.close()

    async def test_market_order_snaps_quantity_and_reports_fill(self):
        fake = FakeBinance()
        ad = _adapter(fake)
        await ad.start()
        try:
            rep = await ad.submit(OrderRequest(symbol=SYMBOL, side="BUY", order_type="MARKET",
                                               quantity=0.0123456, client_order_id="ENTRY-T1"))
            assert rep.status == OrderStatus.FILLED.name and rep.executed_quantity == pytest.approx(0.012)
            sent = [c for c in fake.calls if c[1] == "/fapi/v1/order"][-1][2]
            assert sent["quantity"] == "0.012" and sent["newOrderRespType"] == "RESULT"
            assert "timeInForce" not in sent   # Binance rejects unneeded params (-1106)
        finally:
            await ad.close()

    async def test_stop_goes_to_algo_service_on_tick_grid(self):
        fake = FakeBinance()
        ad = _adapter(fake)
        await ad.start()
        try:
            fake.position = 0.01
            rep = await ad.submit(OrderRequest(
                symbol=SYMBOL, side="SELL", order_type=OrderType.STOP_MARKET.name, quantity=0.01,
                stop_price=49_500.037, reduce_only=True, client_order_id="SL-T1"))
            assert rep.status == OrderStatus.NEW.name
            sent = fake.algos["SL-T1"]
            assert sent["triggerPrice"] == "49500.0" and sent["reduceOnly"] is True
            assert not any(c[1] == "/fapi/v1/order" and c[2].get("type") == "STOP_MARKET"
                           for c in fake.calls)
            opens = await ad.open_orders()
            assert [r.client_order_id for r in opens] == ["SL-T1"]
        finally:
            await ad.close()

    async def test_triggered_algo_reports_fill_of_actual_order(self):
        fake = FakeBinance()
        ad = _adapter(fake)
        q = ad.subscribe()
        await ad.start()
        try:
            fake.position = 0.01
            await ad.submit(OrderRequest(symbol=SYMBOL, side="SELL", order_type="STOP_MARKET",
                                         quantity=0.01, stop_price=49_500.0, reduce_only=True,
                                         client_order_id="SL-T2"))
            fake.trigger("SL-T2", 49_490.0)
            got = []
            assert await _wait(lambda: (got.extend(_drain(q)) or
                                        any(r.status == "FILLED" for r in got)))
            filled = next(r for r in got if r.status == "FILLED")
            assert filled.client_order_id == "SL-T2"
            assert filled.executed_quantity == pytest.approx(0.01)
            assert filled.avg_price == pytest.approx(49_490.0)
        finally:
            await ad.close()

    async def test_cancel_of_already_gone_order_returns_venue_truth(self):
        fake = FakeBinance()
        ad = _adapter(fake)
        await ad.start()
        try:
            await ad.submit(OrderRequest(symbol=SYMBOL, side="BUY", order_type="MARKET",
                                         quantity=0.01, client_order_id="ENTRY-T3"))
            rep = await ad.cancel(SYMBOL, "ENTRY-T3")   # already FILLED
            assert rep.status == OrderStatus.FILLED.name
            with pytest.raises(OrderNotFoundError):
                await ad.cancel(SYMBOL, "ENTRY-NEVER")
        finally:
            await ad.close()


def _drain(q: asyncio.Queue):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


class TestFullLifecycleOnFakeVenue:
    async def test_open_protect_stop_out(self):
        fake = FakeBinance(price=50_000.0)
        ad = _adapter(fake)
        await ad.start()
        om = OrderManager(ad, ExecutionConfig(retries=1, backoff_base_s=0.0))
        pm = PositionManager(om, ExecutionConfig())
        pm.filters = ad.filters
        await pm.start()
        try:
            signal = Signal(symbol=SYMBOL, timestamp_ms=1, signal_type=SignalType.LONG,
                            score=70.0, eligible=True, regime="trending_up")
            decision = RiskDecision(symbol=SYMBOL, ts_ms=1, verdict=RiskVerdict.APPROVED.name,
                                    position_size=0.02, stop_loss_price=49_500.0,
                                    take_profit_price=51_000.0, notional_value=1_000.0,
                                    risk_amount=10.0)
            pos = await pm.open(signal=signal, decision=decision, entry_ref_price=50_000.0)
            assert pos.status is PositionStatus.ACTIVE
            assert set(fake.algos) == {pos.stop_client_order_id, pos.take_profit_client_order_id}
            assert fake.position == pytest.approx(0.02)

            fake.trigger(pos.stop_client_order_id, 49_480.0)
            assert await _wait(lambda: pos.status is PositionStatus.CLOSED)
            assert pos.close_reason == "stop_loss"
            assert pos.realized_pnl == pytest.approx((49_480.0 - 50_000.0) * 0.02)
            # the sibling take-profit is cancelled on the venue
            assert await _wait(lambda: fake.algos[pos.take_profit_client_order_id]["algoStatus"] == "CANCELED")
            assert pm.daily_realized_pnl < pos.realized_pnl   # net of fees
        finally:
            await pm.close()
            await ad.close()

    async def test_manual_close_when_venue_already_flat(self):
        fake = FakeBinance(price=50_000.0)
        ad = _adapter(fake)
        await ad.start()
        om = OrderManager(ad, ExecutionConfig(retries=0))
        pm = PositionManager(om, ExecutionConfig())
        await pm.start()
        try:
            signal = Signal(symbol=SYMBOL, timestamp_ms=1, signal_type=SignalType.SHORT,
                            score=30.0, eligible=True, regime="trending_down")
            decision = RiskDecision(symbol=SYMBOL, ts_ms=1, verdict=RiskVerdict.APPROVED.name,
                                    position_size=0.01, stop_loss_price=50_500.0,
                                    take_profit_price=49_000.0, notional_value=500.0,
                                    risk_amount=5.0)
            pos = await pm.open(signal=signal, decision=decision, entry_ref_price=50_000.0)
            fake.position = 0.0   # flattened out-of-band (e.g. liquidation / manual UI)
            await pm.close_position(pos.position_id, reason="manual")
            assert pos.status is PositionStatus.CLOSED
            assert pos.close_reason == "manual_already_flat"
        finally:
            await pm.close()
            await ad.close()
