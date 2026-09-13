"""Local L2 Order Book synchronized via diff stream + REST snapshot.

Correctly handles the Binance diff-depth synchronization protocol:
  1. buffer diff events while waiting for snapshot
  2. apply snapshot (lastUpdateId)
  3. replay buffered events; discard stale, resync on gap (pu mismatch)
  4. live: drop u <= lastUpdateId; apply where U <= lastUpdateId+1 <= u
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from crypto_scalper.core.models import DiffDepthEvent, OrderBookMetrics
from crypto_scalper.core.exceptions import DataIntegrityError, OrderBookGapError

log = logging.getLogger(__name__)

_MAX_DIFF_BUFFER = 1000
_DEPTH_PCT_BUCKETS = (0.0005, 0.0010, 0.0025)


class OrderBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: Dict[str, float] = {}   # price_str → qty
        self._asks: Dict[str, float] = {}
        self._last_update_id: int = 0
        self._has_snapshot: bool = False
        self.sync_required: bool = True
        self._diff_buffer: List[DiffDepthEvent] = []
        self._last_event_ts_ms: int = 0

    @property
    def last_update_id(self) -> int:
        return self._last_update_id

    @property
    def has_snapshot(self) -> bool:
        return self._has_snapshot

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = 0
        self._has_snapshot = False
        self.sync_required = True
        self._diff_buffer.clear()

    def apply_snapshot(
        self,
        last_update_id: int,
        bids: List[List[str]],
        asks: List[List[str]],
        ts_ms: int,
    ) -> bool:
        """Load full snapshot then attempt to replay buffered diffs.

        Returns True if fully synchronized; False if a gap was detected
        (caller must trigger a re-snapshot).
        """
        self._bids = {}
        self._asks = {}
        for p_str, q_str in bids:
            q = float(q_str)
            if q > 0.0:
                self._bids[_price_key(p_str)] = q
            # q == 0 means price removed, already omitted
        for p_str, q_str in asks:
            q = float(q_str)
            if q > 0.0:
                self._asks[_price_key(p_str)] = q
        self._last_update_id = last_update_id
        self._has_snapshot = True
        self.sync_required = False

        self._replay_buffer()
        self._diff_buffer.clear()
        self._last_event_ts_ms = ts_ms
        return not self.sync_required

    def _replay_buffer(self) -> None:
        for event in list(self._diff_buffer):
            self._apply_diff(event)

    def apply_diff(self, event: DiffDepthEvent) -> None:
        if not self._has_snapshot:
            self._diff_buffer.append(event)
            if len(self._diff_buffer) > _MAX_DIFF_BUFFER:
                log.warning(
                    "orderbook diff buffer overflow",
                    extra={"symbol": self.symbol, "last_update_id": self._last_update_id},
                )
                self.sync_required = True
            return

        self._apply_diff(event)

    def _apply_diff(self, event: DiffDepthEvent) -> None:
        uid = event.final_update_id
        if uid <= self._last_update_id:
            return  # stale
        if not (event.first_update_id <= self._last_update_id + 1 <= uid):
            log.info(
                "orderbook diff gap detected",
                extra={
                    "symbol": self.symbol,
                    "expected_first": self._last_update_id + 1,
                    "got_first": event.first_update_id,
                    "got_final": uid,
                },
            )
            self.sync_required = True
            return
        if event.previous_final_update_id != self._last_update_id:
            log.info(
                "orderbook pu continuity break",
                extra={
                    "symbol": self.symbol,
                    "expected_pu": self._last_update_id,
                    "got_pu": event.previous_final_update_id,
                },
            )
            self.sync_required = True
            return
        _apply_levels(event.bids, self._bids)
        _apply_levels(event.asks, self._asks)
        self._last_update_id = uid
        self._last_event_ts_ms = event.event_time_ms

    def metrics(self, depth_pct_buckets: Tuple[float, ...] = _DEPTH_PCT_BUCKETS) -> OrderBookMetrics:
        if not self._has_snapshot:
            raise DataIntegrityError(f"{self.symbol}: no snapshot available")

        bids = _sorted(self._bids, desc=True)
        asks = _sorted(self._asks, desc=False)
        if not bids or not asks:
            raise DataIntegrityError(f"{self.symbol}: empty book")

        best_bid_p = float(bids[0][0])
        best_ask_p = float(asks[0][0])
        mid = (best_bid_p + best_ask_p) / 2.0
        spread = best_ask_p - best_bid_p
        spread_pct = spread / mid if mid else 0.0

        bid_depth = sum(q for _, q in bids)
        ask_depth = sum(q for _, q in asks)
        denom = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / denom if denom > 0 else 0.0

        top_bid_qty = bids[0][1]
        top_ask_qty = asks[0][1]
        top_denom = top_bid_qty + top_ask_qty
        microprice = (
            (best_bid_p * top_ask_qty + best_ask_p * top_bid_qty) / top_denom
            if top_denom > 0
            else mid
        )

        levels = len(bids) + len(asks)

        depth_buckets = {}
        for bucket in depth_pct_buckets:
            d_b = sum(q for p, q in bids if float(p) >= mid * (1.0 - bucket))
            d_a = sum(q for p, q in asks if float(p) <= mid * (1.0 + bucket))
            depth_buckets[bucket] = d_b + d_a

        return OrderBookMetrics(
            symbol=self.symbol,
            ts_ms=self._last_event_ts_ms,
            best_bid=best_bid_p,
            best_ask=best_ask_p,
            spread=spread,
            spread_pct=spread_pct,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            imbalance=imbalance,
            microprice=microprice,
            levels=levels,
            depth_pct0005=depth_buckets.get(0.0005),
            depth_pct0010=depth_buckets.get(0.0010),
            depth_pct0025=depth_buckets.get(0.0025),
        )


def _apply_levels(levels: Tuple[Tuple[float, float], ...], book: Dict[str, float]) -> None:
    for price_f, qty in levels:
        p = _price_key(price_f)
        q = float(qty)  # ropes in string prices/quantities defensively
        if q == 0.0:
            book.pop(p, None)
        else:
            book[p] = q


def _price_key(p: float) -> str:
    # Normalize any float or numeric string (snapshot or diff) to a single
    # canonical form so levels match across both sources.
    p = float(p)
    if p == 0:
        return "0"
    return f"{p:.10g}"


def _sorted(book: Dict[str, float], desc: bool) -> List[Tuple[str, float]]:
    items = list(book.items())
    items.sort(key=lambda kv: float(kv[0]), reverse=desc)
    return items