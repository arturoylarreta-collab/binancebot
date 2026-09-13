"""ORDER FLOW-category features.

Derived from the TradeAggregator window buckets: taker flow deltas,
imbalance of aggressive flow, trade intensity and quote throughput.
Pure and windowed at feature time; no order placement logic here.
"""

from __future__ import annotations

from typing import Dict

from crypto_scalper.market_data.trades import TradeAggregator

_EPS = 1e-12


def orderflow_features(aggregator: TradeAggregator, now_ms: int) -> Dict[str, float]:
    stats = aggregator.metrics(now_ms)
    s5 = stats[5]
    s30 = stats[30]
    total5 = s5.volume or _EPS
    total30 = s30.volume or _EPS
    delta5 = s5.buy_qty - s5.sell_qty
    delta30 = s30.buy_qty - s30.sell_qty
    return {
        "candle_delta_5s": delta5,
        "candle_delta_30s": delta30,
        "flow_imbalance_5s": delta5 / total5,
        "flow_imbalance_30s": delta30 / total30,
        "aggressive_buy_ratio_30s": s30.buy_ratio,
        "trades_per_second_5s": s5.count / 5.0,
        "avg_trade_size_5s": s5.avg_trade_size,
        "buy_volume_5s": s5.buy_qty,
        "sell_volume_5s": s5.sell_qty,
        "quote_volume_30s": s30.quote_volume,
    }