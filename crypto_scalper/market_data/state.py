"""Per-symbol aggregated state (the 'State' stage of the pipeline).

Holds the coherent local view produced by the processor: order book,
trade buckets, candles, and freshness bookkeeping.
"""

from __future__ import annotations

import time
from typing import Optional

from crypto_scalper.core.models import AggTrade, DiffDepthEvent
from crypto_scalper.market_data.candles import CandleBuilder
from crypto_scalper.market_data.orderbook import OrderBook
from crypto_scalper.market_data.trades import TradeAggregator


class SymbolState:
    def __init__(
        self,
        symbol: str,
        metric_prefix: str = "",
    ) -> None:
        self.symbol = symbol
        self.orderbook = OrderBook(symbol)
        self.trades = TradeAggregator(symbol=symbol)
        self.candles = CandleBuilder(symbol, interval_s=1)
        self.latest_price: Optional[float] = None
        self.last_trade_ts_ms: int = 0
        self.last_ob_ts_ms: int = 0
        self.suspended: bool = False

    def on_trade(self, trade: AggTrade) -> None:
        self.latest_price = trade.price
        self.last_trade_ts_ms = trade.event_time_ms
        self.trades.add(trade, now_ms=trade.event_time_ms)
        self.candles.add(trade)

    def on_depth(self, diff: DiffDepthEvent) -> bool:
        """Apply a depth diff; returns True when a resync is required."""
        self.orderbook.apply_diff(diff)
        self.last_ob_ts_ms = diff.event_time_ms
        return self.orderbook.sync_required

    def is_stale(self, now_ms: int, max_age_ms: int) -> bool:
        latest = max(self.last_trade_ts_ms, self.last_ob_ts_ms)
        return latest > 0 and (now_ms - latest) > max_age_ms

    def is_ready(self) -> bool:
        return (
            self.orderbook.has_snapshot
            and self.latest_price is not None
            and self.candles.count >= 2
        )

    def utc_now_ms(self) -> int:
        return int(time.time() * 1000)