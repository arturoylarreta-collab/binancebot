"""VOLUME-category features built on the TradeAggregator buckets."""

from __future__ import annotations

from typing import Dict

from crypto_scalper.market_data.trades import TradeAggregator


def volume_features(aggregator: TradeAggregator, now_ms: int, window_s: int = 60) -> Dict[str, float]:
    all_stats = aggregator.metrics(now_ms)
    vm = aggregator.volume_metrics(now_ms, window_s=window_s)
    stats_30 = all_stats[30]
    stats_5 = all_stats[5]
    stats_5_vol = max(stats_5.volume, 1e-12)

    # Aggressive volume = all executed volume (from the taker side).
    aggressive = stats_30.volume
    return {
        "buy_volume": stats_30.buy_qty,
        "sell_volume": stats_30.sell_qty,
        "volume_30s": stats_30.volume,
        "volume_60s": vm["volume"],
        "relative_volume": vm["relative_volume"],
        "volume_zscore": vm["volume_zscore"],
        "volume_acceleration": vm["volume_acceleration"],
        "trade_count_30s": float(stats_30.count),
        "avg_trade_size_30s": stats_30.avg_trade_size,
        "aggressive_volume_30s": aggressive,
        "buy_ratio_30s": stats_30.buy_ratio,
        "volume_burst_5s_ratio": stats_5.volume / stats_5_vol,
        "volume_divergence": _divergence(vm["volume"], vm["volume_acceleration"]),
    }


def _divergence(volume: float, acceleration: float) -> float:
    """Positive when volume is rising while price momentum is weak;
    negative when volume falls. Simplified proxy; refined in FASE 3."""
    if volume <= 0:
        return 0.0
    return acceleration / volume