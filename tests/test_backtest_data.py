"""FASE 8 — backtest data: kline→trades, synthetic book, CSV round trip, corpus."""

from __future__ import annotations

import pytest

from crypto_scalper.backtest.data import (
    KlineCorpus,
    klines_csv_path,
    kline_to_trades,
    load_klines_csv,
    parse_klines,
    save_klines_csv,
    synthetic_orderbook,
)
from crypto_scalper.core.models import Candle


def _candle(ts_ms, interval_s=60, o=100.0, h=110.0, l=95.0, c=105.0, vol=10.0):
    return Candle(
        symbol="BTCUSDT", ts_ms=ts_ms, interval_s=interval_s,
        open=o, high=h, low=l, close=c, volume=vol,
        quote_volume=c * vol, trade_count=50, completed=True,
    )


class TestKlineToTrades:
    def test_deterministic_path_inside_range(self):
        candle = _candle(1_000_000, o=100, h=110, l=95, c=105)
        t1 = kline_to_trades("BTCUSDT", candle, steps=20)
        t2 = kline_to_trades("BTCUSDT", candle, steps=20)
        assert [t.price for t in t1] == [t.price for t in t2]
        assert len(t1) == 20
        assert max(t.price for t in t1) <= 110.0 + 1e-9
        assert min(t.price for t in t1) >= 95.0 - 1e-9
        ts = [t.event_time_ms for t in t1]
        assert ts == sorted(ts)
        assert ts[-1] <= candle.ts_ms + candle.interval_s * 1000

    def test_start_toward_high_and_end_near_close(self):
        candle = _candle(5_000, o=100, h=110, l=90, c=109)
        trades = kline_to_trades("BTCUSDT", candle, steps=30)
        assert trades[5].price > 100.0          # early segment rises toward high
        assert abs(trades[-1].price - 109.0) < 1e-6  # last trade lands on close

    def test_volume_split_matches_kline(self):
        candle = _candle(5_000, vol=12.0)
        trades = kline_to_trades("BTCUSDT", candle, steps=24)
        assert sum(t.quantity for t in trades) == pytest.approx(candle.volume)

    def test_flat_bar_is_constant(self):
        candle = _candle(5_000, o=50, h=50, l=50, c=50)
        trades = kline_to_trades("BTCUSDT", candle, steps=5)
        assert all(t.price == 50.0 for t in trades)
        assert all(t.aggressor.name in ("BUY", "SELL") for t in trades)


class TestSyntheticOrderbook:
    def test_two_levels_around_close(self):
        candle = _candle(0, o=100, h=110, l=90, c=100, vol=20)
        bids, asks = synthetic_orderbook(candle, levels=2)
        assert len(bids) == 2 and len(asks) == 2
        assert all(b[0] < candle.close for b in bids)
        assert all(a[0] > candle.close for a in asks)
        assert all(pytest.approx(b[1]) == 10.0 for b in bids)   # volume / levels
        assert asks[0][0] > bids[0][0]                          # positive spread

    def test_non_flat_bar_has_positive_spread(self):
        candle = _candle(0, o=100, h=102, l=98, c=100)
        bids, asks = synthetic_orderbook(candle)
        assert asks[0][0] > bids[0][0]


class TestCsvRoundTrip:
    def test_round_trip_equality(self, tmp_path):
        candles = [_candle(1_000 + i * 60_000) for i in range(5)]
        path = klines_csv_path(tmp_path, "BTCUSDT", 60)
        save_klines_csv(path, candles)
        loaded = load_klines_csv(path, "BTCUSDT", 60)
        assert loaded == candles

    def test_load_rejects_empty_file(self, tmp_path):
        path = klines_csv_path(tmp_path, "BTCUSDT", 60)
        path.write_text("symbol,open_time_ms,open,high,low,close,volume,quote_volume,trade_count\n")
        with pytest.raises(Exception):
            load_klines_csv(path, "BTCUSDT", 60)


class TestParseKlines:
    def test_raw_binance_array_row(self):
        row = [
            "1700000000000", "100.0", "110.0", "95.0", "105.0", "10.0",
            "1700000060000", "1050.0", "300", "5.0", "525.0", "0",
        ]
        candles = parse_klines([row], "BTCUSDT", 60)
        assert len(candles) == 1
        assert candles[0].close == 105.0
        assert candles[0].volume == 10.0
        assert candles[0].trade_count == 300


class TestKlineCorpus:
    def test_interleaves_chronologically(self):
        a = [_candle(3_000), _candle(6_000)]
        b = [_candle(1_500), _candle(4_500)]
        corpus = KlineCorpus({"A": a, "B": b})
        events = list(corpus.events())
        assert [e.candle.ts_ms for e in events] == [1_500, 3_000, 4_500, 6_000]
        assert [e.symbol for e in events] == ["B", "A", "B", "A"]

    def test_dedupes_duplicate_timestamps(self):
        dup = [_candle(1_000), _candle(1_000), _candle(2_000)]
        corpus = KlineCorpus({"A": dup})
        assert len(list(corpus.events())) == 2

    def test_rejects_mixed_intervals(self):
        with pytest.raises(Exception):
            KlineCorpus({"A": [_candle(0, interval_s=60), _candle(60_000, interval_s=300)]})