"""Offline tests for the synchronized L2 Order Book."""

import pytest

from crypto_scalper.core.models import DiffDepthEvent
from crypto_scalper.market_data.orderbook import OrderBook


def diff(
    symbol: str,
    first: int,
    final: int,
    pu: int,
    bids=(),
    asks=(),
    ts: int = 0,
) -> DiffDepthEvent:
    return DiffDepthEvent(
        symbol=symbol,
        event_time_ms=ts,
        first_update_id=first,
        final_update_id=final,
        previous_final_update_id=pu,
        bids=bids,
        asks=asks,
    )


class TestOrderBookSync:
    def test_snapshot_then_diff(self):
        ob = OrderBook("BTCUSDT")
        ok = ob.apply_snapshot(100, [["100", "1"], ["99", "2"]], [["101", "3"], ["102", "4"]], ts_ms=1)
        assert ok
        assert ob.has_snapshot
        assert ob.last_update_id == 100
        assert ob.sync_required is False

        ev = diff("BTCUSDT", first=101, final=101, pu=100, bids=(("99.5", "5"),))
        ob.apply_diff(ev)
        assert ob.last_update_id == 101
        m = ob.metrics()
        assert m.best_bid == 100.0      # best level unchanged
        assert m.bid_depth == 8.0       # 1 + 2 + new 99.5 level (5)

    def test_stale_event_dropped(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        ev = diff("BTCUSDT", first=99, final=99, pu=98, bids=(("99", "9"),))
        ob.apply_diff(ev)
        assert ob.last_update_id == 100
        assert ob.metrics().best_bid == 100.0  # stale level not applied


class TestOrderBookGap:
    def test_gap_detected(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        # first_update_id jumps over last_update_id+1
        ev = diff("BTCUSDT", first=103, final=104, pu=100)
        ob.apply_diff(ev)
        assert ob.sync_required is True
        assert ob.last_update_id == 100  # not applied

    def test_pu_mismatch_detected(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        ob.apply_diff(diff("BTCUSDT", first=101, final=101, pu=100))
        ev = diff("BTCUSDT", first=103, final=104, pu=102)  # skipped u=102
        ob.apply_diff(ev)
        assert ob.sync_required is True
        assert ob.last_update_id == 101

    def test_first_event_straddles_snapshot(self):
        # Binance futures: the first event only needs U <= lastUpdateId <= u;
        # its pu refers to a pre-snapshot event and must NOT be checked.
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(105, [["100", "1"]], [["101", "1"]], ts_ms=1)
        ob.apply_diff(diff("BTCUSDT", first=100, final=110, pu=95, bids=(("99", "2"),)))
        assert ob.sync_required is False
        assert ob.last_update_id == 110
        ob.apply_diff(diff("BTCUSDT", first=111, final=112, pu=110))
        assert ob.is_synced and ob.last_update_id == 112

    def test_resync_buffers_diffs_in_flight(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        ob.begin_resync()
        assert not ob.has_snapshot and not ob.is_synced
        ob.apply_diff(diff("BTCUSDT", first=190, final=199, pu=189))   # stale vs new snapshot
        ob.apply_diff(diff("BTCUSDT", first=200, final=210, pu=199, bids=(("99", "4"),)))
        ok = ob.apply_snapshot(205, [["100", "1"]], [["101", "1"]], ts_ms=2)
        assert ok and ob.last_update_id == 210
        assert ob.metrics().bid_depth == 5.0

    def test_no_apply_while_out_of_sync(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        ob.apply_diff(diff("BTCUSDT", first=150, final=151, pu=149))  # gap
        assert ob.sync_required
        ob.apply_diff(diff("BTCUSDT", first=152, final=152, pu=151, bids=(("99", "9"),)))
        assert ob.last_update_id == 100

    def test_buffered_events_replayed_after_snapshot(self):
        ob = OrderBook("BTCUSDT")
        buffered = diff("BTCUSDT", first=101, final=101, pu=100, bids=(("99.5", "5"),))
        ob.apply_diff(buffered)  # no snapshot yet -> buffered
        assert ob.has_snapshot is False

        ok = ob.apply_snapshot(100, [["100", "1"]], [["101", "1"]], ts_ms=1)
        assert ok
        assert ob.last_update_id == 101
        assert not ob.sync_required
        m = ob.metrics()
        assert m.best_bid == 100.0      # best level unchanged, 99.5 now present
        assert m.bid_depth == 6.0

    def test_zero_qty_removes_level(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(100, [["100", "1"], ["99", "2"]], [["101", "1"]], ts_ms=1)
        ev = diff("BTCUSDT", first=101, final=101, pu=100, bids=(("100", "0"), ("98", "3")))
        ob.apply_diff(ev)
        m = ob.metrics()
        assert m.best_bid == 99
        assert m.bid_depth == 5


class TestOrderBookMetrics:
    def _book(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(
            10,
            [["99", "2"], ["98", "1"]],
            [["101", "1"], ["102", "2"]],
            ts_ms=1,
        )
        return ob

    def test_quotes(self):
        m = self._book().metrics()
        assert m.best_bid == 99.0
        assert m.best_ask == 101.0
        assert m.spread == 2.0
        assert abs(m.spread_pct - 0.02) < 1e-9

    def test_imbalance_symmetric(self):
        m = self._book().metrics()
        assert m.imbalance == 0.0

    def test_imbalance_buy_heavy(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(10, [["99", "9"], ["98", "1"]], [["101", "1"]], ts_ms=1)
        m = ob.metrics()
        assert m.imbalance > 0.0

    def test_microprice(self):
        # microprice = (bid*askQty + ask*bidQty)/(bidQty+askQty) at best level
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(10, [["99", "2"]], [["101", "1"]], ts_ms=1)
        m = ob.metrics()
        assert abs(m.microprice - (99.0 * 1 + 101.0 * 2) / 3.0) < 1e-9

    def test_depth_buckets(self):
        ob = OrderBook("BTCUSDT")
        ob.apply_snapshot(
            10,
            [["100", "5"], ["99.8", "1"], ["50", "1"]],
            [["100.2", "5"], ["100.5", "1"], ["150", "1"]],
            ts_ms=1,
        )
        m = ob.metrics(depth_pct_buckets=(0.0010,))
        # ±0.1% of mid (~100.1) -> only 100 and 100.2 qualify
        assert m.depth_pct0005 is None
        assert m.depth_pct0010 == pytest.approx(5 + 5)
        assert m.depth_pct0025 is None

    def test_no_snapshot_raises(self):
        ob = OrderBook("BTCUSDT")
        with pytest.raises(Exception):
            ob.metrics()