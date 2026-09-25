"""Local L2 Order Book synchronized via diff stream + REST snapshot.

Implements the Binance USD-M Futures "manage a local order book" protocol:

  1. while no snapshot is loaded, buffer diff events (bounded, newest kept)
  2. load the REST snapshot (``lastUpdateId``)
  3. drop any event with ``u < lastUpdateId``
  4. the first applied event must satisfy ``U <= lastUpdateId + 1 <= u + 1``
  5. every following event must satisfy ``pu == previous u``; otherwise the
     book is out of sync and the caller must :meth:`begin_resync`

``begin_resync`` clears the book and re-enables buffering so diffs that arrive
while the REST snapshot is in flight are replayed instead of lost.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Deque, Dict, List, Tuple

from crypto_scalper.core.models import DiffDepthEvent, OrderBookMetrics
from crypto_scalper.core.exceptions import DataIntegrityError

log = logging.getLogger(__name__)

_MAX_DIFF_BUFFER = 1000
_MAX_LEVELS_PER_SIDE = 1000
_METRIC_LEVELS = 20
_DEPTH_PCT_BUCKETS = (0.0005, 0.0010, 0.0025)


class OrderBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: Dict[str, float] = {}   # price_key → qty
        self._asks: Dict[str, float] = {}
        self._last_update_id: int = 0
        self._has_snapshot: bool = False
        self._first_applied: bool = False
        self.sync_required: bool = True
        self._diff_buffer: Deque[DiffDepthEvent] = deque(maxlen=_MAX_DIFF_BUFFER)
        self._last_event_ts_ms: int = 0
        self.resyncs: int = 0

    @property
    def last_update_id(self) -> int:
        return self._last_update_id

    @property
    def has_snapshot(self) -> bool:
        return self._has_snapshot

    @property
    def is_synced(self) -> bool:
        return self._has_snapshot and not self.sync_required

    @property
    def last_event_ts_ms(self) -> int:
        return self._last_event_ts_ms

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = 0
        self._has_snapshot = False
        self._first_applied = False
        self.sync_required = True
        self._diff_buffer.clear()

    def begin_resync(self) -> None:
        """Drop the (inconsistent) book and buffer diffs until the next snapshot.

        Already-buffered diffs are KEPT: a REST snapshot can lag the stream, and
        those diffs are exactly what bridges it (stale ones are dropped on replay).
        """
        self.resyncs += 1
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = 0
        self._has_snapshot = False
        self._first_applied = False
        self.sync_required = True

    def apply_snapshot(
        self,
        last_update_id: int,
        bids: List[List[str]],
        asks: List[List[str]],
        ts_ms: int,
    ) -> bool:
        """Load a full snapshot then replay buffered diffs.

        Returns True if fully synchronized; False if a gap was detected while
        replaying (caller must trigger a re-snapshot).
        """
        self._bids = {_price_key(p): float(q) for p, q in bids if float(q) > 0.0}
        self._asks = {_price_key(p): float(q) for p, q in asks if float(q) > 0.0}
        self._last_update_id = last_update_id
        self._has_snapshot = True
        self._first_applied = False
        self.sync_required = False
        self._last_event_ts_ms = ts_ms

        buffered = list(self._diff_buffer)
        self._diff_buffer.clear()
        for event in buffered:
            self._apply_diff(event)
            if self.sync_required:
                break
        return not self.sync_required

    def apply_partial(self, event: DiffDepthEvent) -> None:
        """Replace the book with a top-N snapshot from a partial-depth stream."""
        self._bids = {_price_key(p): float(q) for p, q in event.bids if float(q) > 0.0}
        self._asks = {_price_key(p): float(q) for p, q in event.asks if float(q) > 0.0}
        self._last_update_id = event.final_update_id
        self._has_snapshot = bool(self._bids and self._asks)
        self._first_applied = True
        self.sync_required = not self._has_snapshot
        self._last_event_ts_ms = event.event_time_ms
        self._diff_buffer.clear()

    def apply_diff(self, event: DiffDepthEvent) -> None:
        if getattr(event, "is_snapshot", False):
            self.apply_partial(event)
            return
        if not self._has_snapshot:
            self._diff_buffer.append(event)  # bounded deque: oldest drops first
            return
        if self.sync_required:
            return  # waiting for the caller to resync; applying would corrupt
        self._apply_diff(event)

    def _apply_diff(self, event: DiffDepthEvent) -> None:
        u = event.final_update_id
        if not self._first_applied:
            if u < self._last_update_id:
                return  # older than the snapshot
            if event.first_update_id > self._last_update_id + 1:
                log.info(
                    "orderbook first diff gap",
                    extra={"symbol": self.symbol, "snapshot_id": self._last_update_id,
                           "got_first": event.first_update_id, "got_final": u},
                )
                self.sync_required = True
                return
            self._first_applied = True
        else:
            if u <= self._last_update_id:
                return  # duplicate / replayed
            if event.previous_final_update_id != self._last_update_id:
                log.info(
                    "orderbook pu continuity break",
                    extra={"symbol": self.symbol, "expected_pu": self._last_update_id,
                           "got_pu": event.previous_final_update_id},
                )
                self.sync_required = True
                return
        _apply_levels(event.bids, self._bids)
        _apply_levels(event.asks, self._asks)
        self._last_update_id = u
        self._last_event_ts_ms = event.event_time_ms
        if len(self._bids) > _MAX_LEVELS_PER_SIDE or len(self._asks) > _MAX_LEVELS_PER_SIDE:
            self._prune()

    def _prune(self) -> None:
        """Keep the book bounded: far-from-touch levels never matter for scalping."""
        self._bids = dict(_sorted(self._bids, desc=True)[:_MAX_LEVELS_PER_SIDE // 2])
        self._asks = dict(_sorted(self._asks, desc=False)[:_MAX_LEVELS_PER_SIDE // 2])

    def metrics(self, depth_pct_buckets: Tuple[float, ...] = _DEPTH_PCT_BUCKETS) -> OrderBookMetrics:
        if not self._has_snapshot:
            raise DataIntegrityError(f"{self.symbol}: no snapshot available")

        bids = _sorted(self._bids, desc=True)[:_METRIC_LEVELS * 5]
        asks = _sorted(self._asks, desc=False)[:_METRIC_LEVELS * 5]
        if not bids or not asks:
            raise DataIntegrityError(f"{self.symbol}: empty book")

        best_bid_p = float(bids[0][0])
        best_ask_p = float(asks[0][0])
        mid = (best_bid_p + best_ask_p) / 2.0
        spread = best_ask_p - best_bid_p
        spread_pct = spread / mid if mid else 0.0

        bid_depth = sum(q for _, q in bids[:_METRIC_LEVELS])
        ask_depth = sum(q for _, q in asks[:_METRIC_LEVELS])
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