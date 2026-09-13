"""Universe Selector: builds the tradable universe from live market data.

Not a static top-100 list. Produces a Tradability Score from quote volume,
spread, depth and trade activity, with hard filters for untradeable
instruments (low liquidity, wide spread, extreme moves, blocking rules).

REST-only (used at startup and on a periodic refresh); the WebSocket layer
never depends on this file.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

from crypto_scalper.config.settings import UniverseConfig
from crypto_scalper.config.symbols import SymbolRules
from crypto_scalper.core.exceptions import ExchangeConnectionError
from crypto_scalper.market_data.rest import BinanceFuturesRest
from crypto_scalper.monitoring.metrics import Metrics

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class UniverseSymbol:
    symbol: str
    base_asset: str
    quote_volume_24h: float
    volume_24h: float
    price_change_pct: float
    trade_count_24h: int
    spread_pct: float
    depth_0001: float
    est_slippage_pct: float
    tradability_score: float
    blocked_reason: str = ""


class UniverseSelector:
    def __init__(
        self,
        config: UniverseConfig,
        rules: SymbolRules,
        rest: BinanceFuturesRest,
        metrics: Metrics,
        depth_limit: int = 20,
        probe_concurrency: int = 16,
    ) -> None:
        self._config = config
        self._rules = rules
        self._rest = rest
        self._metrics = metrics
        self._depth_limit = depth_limit
        self._probe_concurrency = probe_concurrency

    async def select(self) -> List[UniverseSymbol]:
        try:
            tickers = await self._rest.ticker_24h()
        except ExchangeConnectionError as exc:
            log.error("universe ticker fetch failed", extra={"error": str(exc)})
            return []

        info = await self._rest.exchange_info()
        status_by_symbol = {s["symbol"]: s.get("status") for s in info}
        meta_by_symbol = {
            s["symbol"]: {"base": s.get("baseAsset", ""), "quote": s.get("quoteAsset", "")}
            for s in info
        }

        rows = []
        for t in tickers:
            symbol = t["symbol"]
            meta = meta_by_symbol.get(symbol, {})
            base = meta.get("base", "")
            quote = meta.get("quote", "")
            if symbol in self._rules.blocked:
                continue
            if quote not in self._rules.allowed_quotes:
                continue
            if status_by_symbol.get(symbol) not in ("TRADING", None):
                continue
            try:
                quote_volume = float(t.get("quoteVolume") or 0)
                volume = float(t.get("volume") or 0)
                change = float(t.get("priceChangePercent") or 0)
                count = int(t.get("count") or 0)
            except (TypeError, ValueError):
                continue
            if quote_volume < self._config.min_quote_volume_24h:
                continue
            if not (self._config.min_price_change_pct <= change <= self._config.max_price_change_pct):
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "base": base,
                    "quote_volume": quote_volume,
                    "volume": volume,
                    "price_change_pct": change,
                    "trade_count": count,
                }
            )

        probed = await self._probe_depths(rows)
        ranked = self.rank(probed, max_symbols=self._config.max_symbols)
        self._metrics.set_gauge("universe.candidates", len(probed))
        self._metrics.set_gauge("universe.selected", len(ranked))
        log.info(
            "universe selected",
            extra={"candidates": len(probed), "selected": len(ranked)},
        )
        return ranked

    async def _probe_depths(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(self._probe_concurrency)

        async def probe(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    data = await self._rest.depth(row["symbol"], self._depth_limit)
                except ExchangeConnectionError as exc:
                    log.debug("depth probe failed", extra={"symbol": row["symbol"], "error": str(exc)})
                    return None
                bids = _pairs(data.get("bids", []))
                asks = _pairs(data.get("asks", []))
                metrics = _book_metrics(bids, asks, quote_notional=10_000.0)
                if metrics is None:
                    return None
                if metrics["spread_pct"] > self._config.max_spread_pct:
                    return None
                return {**row, **metrics}

        results = await asyncio.gather(*(probe(r) for r in rows))
        return [r for r in results if r is not None]

    @staticmethod
    def rank(rows: Sequence[Dict[str, Any]], max_symbols: int = 100) -> List[UniverseSymbol]:
        """Pure ranking used by tests; no network, no future data.

        Components (0..1 after min-max normalization), weighted by
        UniverseConfig.score_weights: liquidity(quote vol), spread (inverted),
        depth (±0.1%), trade activity.
        """
        if not rows:
            return []

        quote_vol = np.array([r["quote_volume"] for r in rows], dtype=float)
        spread = np.array([r["spread_pct"] for r in rows], dtype=float)
        depth = np.array([r["depth_0001"] for r in rows], dtype=float)
        count = np.array([r["trade_count"] for r in rows], dtype=float)

        n = len(rows)
        w_liquidity = max(0.001, quote_vol.max())
        w_spread = max(0.000001, spread.max())
        w_depth = max(0.001, depth.max())
        w_count = max(1.0, count.max())

        liquidity_s = _safe_log_scale(quote_vol, w_liquidity)
        spread_s = np.clip(1.0 - spread / (w_spread * 2), 0.0, 1.0)
        depth_s = clip01(np.log1p(depth) / max(1e-9, np.log1p(w_depth)))
        count_s = clip01(np.log1p(count) / max(1e-9, np.log1p(w_count)))

        weights = (0.35, 0.30, 0.25, 0.10)
        score = (
            weights[0] * liquidity_s
            + weights[1] * spread_s
            + weights[2] * depth_s
            + weights[3] * count_s
        )

        order = np.argsort(-score)
        out: List[UniverseSymbol] = []
        for i in order[:max_symbols]:
            r = rows[int(i)]
            out.append(
                UniverseSymbol(
                    symbol=r["symbol"],
                    base_asset=r["base"],
                    quote_volume_24h=float(r["quote_volume"]),
                    volume_24h=float(r["volume"]),
                    price_change_pct=float(r["price_change_pct"]),
                    trade_count_24h=int(r["trade_count"]),
                    spread_pct=float(r["spread_pct"]),
                    depth_0001=float(r["depth_0001"]),
                    est_slippage_pct=float(r["est_slippage_pct"]),
                    tradability_score=float(score[int(i)] * 100.0),
                )
            )
        return out


def clip01(a: np.ndarray) -> np.ndarray:
    return np.clip(a, 0.0, 1.0)


def _safe_log_scale(values: np.ndarray, maxv: float) -> np.ndarray:
    return clip01(np.log1p(np.maximum(values, 0.0)) / max(1e-9, np.log1p(maxv)))


def _pairs(rows: Sequence[Sequence[Any]]) -> List[tuple]:
    out = []
    for row in rows:
        try:
            out.append((float(row[0]), float(row[1])))
        except (TypeError, ValueError):
            continue
    return out


def _book_metrics(
    bids: List[tuple],
    asks: List[tuple],
    quote_notional: float,
    depth_pct: float = 0.001,
) -> Optional[Dict[str, float]]:
    """spread_pct, depth_0001 (base qty within ±0.1%), est slippage for
    quote_notional of aggressive buying/theoretical average execution.

    Returns None if the book is invalid or too thin.
    """
    if not bids or not asks:
        return None
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None
    spread_pct = (best_ask - best_bid) / mid

    depth = sum(q for p, q in bids if p >= mid * (1 - depth_pct)) + sum(
        q for p, q in asks if p <= mid * (1 + depth_pct)
    )

    # Estimated slippage for a market buy of quote_notional.
    remaining = quote_notional
    qty = 0.0
    cost = 0.0
    for price, size in asks:
        take = min(size, remaining / price if price > 0 else 0.0)
        qty += take
        cost += take * price
        remaining -= take * price
        if remaining <= 0:
            break
    slippage_pct = 0.0
    if qty > 0 and remaining <= 0:
        avg_exec = cost / qty
        slippage_pct = (avg_exec - mid) / mid
    else:
        # Not enough book to fill the probe order: penalize.
        slippage_pct = 0.5

    return {
        "spread_pct": spread_pct,
        "depth_0001": depth,
        "est_slippage_pct": slippage_pct,
    }