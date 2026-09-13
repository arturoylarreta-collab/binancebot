"""Offline tests for TradeAggregator windowed statistics."""

from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.models import AggTrade
from crypto_scalper.market_data.trades import TradeAggregator

T0 = 1_700_000_000_000  # arbitrary epoch ms, aligned to second


def trade(symbol, idx, price, qty, side, base_ts=T0):
    return AggTrade(
        symbol=symbol,
        event_time_ms=base_ts + idx,
        trade_id=idx,
        price=price,
        quantity=qty,
        aggressor=side,
    )


class TestAggregation:
    def test_windows_and_split(self):
        agg = TradeAggregator()
        # 2 buys + 1 sell in the first second
        agg.add(trade("BTCUSDT", 0, 100.0, 1.0, AggressorSide.BUY), T0)
        agg.add(trade("BTCUSDT", 1, 100.0, 2.0, AggressorSide.BUY), T0 + 10)
        agg.add(trade("BTCUSDT", 2, 100.0, 3.0, AggressorSide.SELL), T0 + 20)
        # 1 buy at +1s
        agg.add(trade("BTCUSDT", 3, 101.0, 4.0, AggressorSide.BUY), T0 + 1000)

        stats = agg.metrics(T0 + 1500)
        s1 = stats[1]     # last 1 second
        s5 = stats[5]     # last 5 seconds includes everything

        assert s1.count == 1
        assert s1.buy_qty == 4.0
        assert s1.sell_qty == 0.0

        assert s5.count == 4
        assert s5.buy_qty == 7.0
        assert s5.sell_qty == 3.0
        assert s5.volume == 10.0
        assert s5.avg_trade_size == 2.5
        assert abs(s5.buy_ratio - 0.7) < 1e-9

    def test_older_buckets_expire(self):
        agg = TradeAggregator(windows_s=(1, 2))
        for i in range(4):
            agg.add(trade("X", i, 10.0, 1.0, AggressorSide.BUY), T0 + i * 1000)
        now = T0 + 3 * 1000 + 500
        stats = agg.metrics(now)
        # Only the final bucket (ts T0+3s) is within the 1s window
        assert stats[1].count == 1
        # Within the 2s window: last two buckets
        assert stats[2].count == 2

    def test_volume_metrics_zscore(self):
        agg = TradeAggregator(windows_s=(5,))
        # feed 20 seconds of uniform volume
        for sec in range(20):
            agg.add(trade("X", sec, 10.0, 1.0, AggressorSide.BUY), T0 + sec * 1000)
        vm = agg.volume_metrics(T0 + 20 * 1000, window_s=5)
        assert vm["relative_volume"] >= 0.0
        assert abs(vm["volume_zscore"]) < 3.0  # not wildly off-uniform

    def test_empty(self):
        agg = TradeAggregator(windows_s=(1, 5))
        stats = agg.metrics(T0)
        assert stats[1].count == 0
        assert stats[5].volume == 0.0
        assert stats[5].avg_trade_size == 0.0

    def test_unknown_aggressor_counts_as_neutral(self):
        agg = TradeAggregator(windows_s=(5,))
        agg.add(trade("X", 0, 10.0, 2.0, AggressorSide.UNKNOWN), T0)
        stats = agg.metrics(T0 + 100)
        # BASE volume still counts; buy/sell split neutral
        assert stats[5].volume == 2.0
        assert stats[5].buy_qty == 0.0
        assert stats[5].sell_qty == 0.0