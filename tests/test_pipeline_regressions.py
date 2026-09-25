"""Regression tests for the FASE 9 audit's P0 findings in the live pipeline.

Each test pins a bug that made paper/pipeline mode silently inert before:
events dropped by the processor, the reconnect storm, legacy stream routing,
the clock offset and the exchange filters wiring in main.
"""

from __future__ import annotations

import asyncio

import pytest

from crypto_scalper.config.settings import Settings
from crypto_scalper.core import clock
from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.events import DepthEvent, EventBus, TradeEvent
from crypto_scalper.core.models import AggTrade, DiffDepthEvent
from crypto_scalper.market_data.processor import SymbolProcessor
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.market_data.websocket import WebSocketManager
from crypto_scalper.monitoring.metrics import Metrics


class _FakeRest:
    def __init__(self, last_update_id: int = 100) -> None:
        self.last_update_id = last_update_id
        self.calls = 0

    async def depth(self, symbol, limit):
        self.calls += 1
        return {"lastUpdateId": self.last_update_id,
                "bids": [["100", "1"]], "asks": [["101", "1"]]}


async def _until(pred, timeout=2.0):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


async def test_processor_consumes_bus_wrappers_and_syncs_book():
    """Before: isinstance(item, AggTrade) on TradeEvent wrappers → every event dropped."""
    bus = EventBus(queue_maxsize=100)
    state = SymbolState("BTCUSDT")
    proc = SymbolProcessor("BTCUSDT", state, _FakeRest(100), bus, Metrics())
    stop = asyncio.Event()
    await proc.start()
    task = asyncio.create_task(proc.run(stop))
    try:
        bus.publish_nowait(DepthEvent(topic="market.BTCUSDT.depth", diff=DiffDepthEvent(
            symbol="BTCUSDT", event_time_ms=1, first_update_id=95, final_update_id=105,
            previous_final_update_id=90, bids=((99.0, 2.0),), asks=())))
        for i in range(3):
            bus.publish_nowait(TradeEvent(topic="market.BTCUSDT.trade", trade=AggTrade(
                symbol="BTCUSDT", event_time_ms=1000 * (i + 1), trade_id=i, price=100.5 + i,
                quantity=0.1, aggressor=AggressorSide.BUY)))
        assert await _until(lambda: state.latest_price == 102.5)
        assert await _until(lambda: state.orderbook.is_synced)
        assert state.orderbook.last_update_id == 105   # straddling diff applied
    finally:
        stop.set()
        await task
        await proc.close()


def test_publish_overflow_does_not_raise_into_the_reader():
    """Before: `await bus.publish_nowait(...)` → TypeError → reconnect storm."""
    bus = EventBus(queue_maxsize=1)
    bus.subscribe("market.BTCUSDT.trade")
    ws = WebSocketManager(Settings.load().ws, bus, Metrics())
    ev = TradeEvent(topic="market.BTCUSDT.trade", trade=None)

    async def _run():
        await ws._publish_checked(ev)
        await ws._publish_checked(ev)   # queue full: dropped + counted, no exception

    asyncio.run(_run())


def test_streams_are_routed_by_category():
    """Binance only serves @aggTrade on /market and @depth on /public."""
    ws = WebSocketManager(Settings.load().ws, EventBus(), Metrics())
    batches = ws.routed_batches(["BTCUSDT", "ETHUSDT"])
    by_url = {url: streams for url, streams in batches}
    market = [u for u in by_url if u.endswith("/market/stream")]
    public = [u for u in by_url if u.endswith("/public/stream")]
    assert market and public
    assert all(s.endswith("@aggTrade") for s in by_url[market[0]])
    assert all(s.endswith("@depth") for s in by_url[public[0]])
    assert not any("/stream/" in u or u.endswith("/stream/stream") for u in by_url)


def test_clock_offset_applies_to_state_freshness():
    clock.set_offset_ms(-30_000)   # host 30 s ahead of the exchange
    try:
        st = SymbolState("BTCUSDT")
        st.last_trade_ts_ms = clock.now_ms() - 200
        assert st.data_age_ms(clock.now_ms()) < 1_000
    finally:
        clock.set_offset_ms(0)


def test_build_observability_repository_accepts_start_equity(tmp_path):
    """Before: run_paper omitted the required start_equity kwarg → TypeError at boot."""
    from crypto_scalper.main import _build_observability_repository
    from crypto_scalper.storage.sqlite_repo import SqliteRepository

    repo, notifier = _build_observability_repository(
        SqliteRepository(tmp_path / "x.db"), Settings.load(), mode="paper",
        alerts=False, start_equity=1234.0)
    assert repo is not None and notifier is not None
    asyncio.run(repo.close())


@pytest.mark.parametrize("raw,expected", [("paper", "paper"), ("testnet", "paper")])
def test_testnet_without_keys_falls_back_to_paper(raw, expected):
    from crypto_scalper.config.venue import VenueConfig
    assert VenueConfig(venue=raw).effective_venue() == expected


def test_secrets_are_redacted_from_final_log_line():
    import logging
    from crypto_scalper.monitoring.logger import KeyValueFormatter
    rec = logging.LogRecord("x", logging.ERROR, __file__, 1,
                            "post https://api.telegram.org/bot123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawx/send",
                            (), None)
    rec.api_secret = "s3cr3t"
    line = KeyValueFormatter("%(message)s").format(rec)
    assert "AAHdqTcv" not in line and "s3cr3t" not in line
