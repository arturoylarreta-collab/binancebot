"""Feature categories for the Feature Engine (FASE 3).

Every category package features by dimension: PRICE, VOLUME, VOLATILITY,
ORDER FLOW, ORDER BOOK, MOMENTUM, TREND, SOCIAL, NEWS, MARKET REGIME.

All builders are pure functions of data available at `now_ms`; none may
reference future information. The whole bundle is timestamped by the
FeatureSnapshot (every category is computed at the same `now_ms`).
"""

from __future__ import annotations

from typing import Dict, Optional

from crypto_scalper.core.enums import Impact, Regime
from crypto_scalper.core.models import NewsEvent, OrderBookMetrics
from crypto_scalper.features import price_features
from crypto_scalper.features.orderflow import orderflow_features
from crypto_scalper.market_data.state import SymbolState

CATEGORY_KEYS = (
    "price",
    "volume",
    "volatility",
    "order_flow",
    "order_book",
    "momentum",
    "trend",
    "social",
    "news",
    "market_regime",
)

_IMPACT_WEIGHT = {
    Impact.HIGH: 1.0,
    Impact.MEDIUM: 0.6,
    Impact.LOW: 0.3,
}


def build_all(
    *,
    state: SymbolState,
    price: float,
    close,
    high,
    low,
    vwap: float,
    ema9: float,
    ema21: float,
    ema50: float,
    rsi: float,
    adx: float,
    roc10: float,
    roc30: float,
    roc60: float,
    boll_upper: float,
    boll_mid: float,
    boll_lower: float,
    atr: float,
    atr_pct: float,
    realized_vol: float,
    vol: Dict[str, float],
    ob: Optional[OrderBookMetrics],
    obi_delta: float,
    news: Optional[NewsEvent],
    mention_zscore: float,
    news_count: int,
    now_ms: int,
    ttl_ms: int,
    regime: Regime,
) -> Dict[str, Dict[str, float]]:
    return {
        "price": price_category(price, close, high, low, vwap, boll_upper, boll_mid, boll_lower),
        "volume": volume_category(vol),
        "volatility": volatility_category(atr, atr_pct, realized_vol, price, boll_upper, boll_lower),
        "order_flow": orderflow_features(state.trades, now_ms),
        "order_book": order_book_category(ob, price, obi_delta),
        "momentum": momentum_category(rsi, roc10, roc30, roc60),
        "trend": trend_category(price, ema9, ema21, ema50, adx, vwap),
        "social": social_category(mention_zscore, news_count),
        "news": news_category(news, now_ms, ttl_ms),
        "market_regime": regime_category(regime, adx, atr_pct),
    }


def price_category(
    price: float,
    close,
    high,
    low,
    vwap: float,
    boll_upper: float,
    boll_mid: float,
    boll_lower: float,
) -> Dict[str, float]:
    out = dict(price_features.price_features(close, high, low))
    out["price_vs_vwap_pct"] = (price - vwap) / vwap if vwap else 0.0
    out["boll_position"] = _boll_position(price, boll_upper, boll_mid, boll_lower)
    return out


def volume_category(vol: Dict[str, float]) -> Dict[str, float]:
    return dict(vol)


def volatility_category(
    atr: float,
    atr_pct: float,
    realized_vol: float,
    price: float,
    boll_upper: float,
    boll_lower: float,
) -> Dict[str, float]:
    width = (boll_upper - boll_lower) / price if price else 0.0
    return {
        "atr": atr,
        "atr_pct": atr_pct,
        "realized_volatility": realized_vol,
        "boll_width_pct": width,
    }


def order_book_category(
    ob: Optional[OrderBookMetrics],
    price: float,
    obi_delta: float,
) -> Dict[str, float]:
    if ob is None:
        return {
            "obi": 0.0,
            "obi_delta": obi_delta,
            "microprice_pressure_pct": 0.0,
            "spread": 0.0,
            "spread_pct": 0.0,
            "bid_depth": 0.0,
            "ask_depth": 0.0,
            "bid_ask_ratio": 0.5,
            "depth_pct0005": 0.0,
            "depth_pct0010": 0.0,
            "depth_pct0025": 0.0,
        }
    denom = ob.bid_depth + ob.ask_depth
    return {
        "obi": float(ob.imbalance),
        "obi_delta": float(obi_delta),
        "microprice_pressure_pct": (ob.microprice - price) / price if price else 0.0,
        "spread": float(ob.spread),
        "spread_pct": float(ob.spread_pct),
        "bid_depth": float(ob.bid_depth),
        "ask_depth": float(ob.ask_depth),
        "bid_ask_ratio": float(ob.bid_depth / denom) if denom > 0 else 0.5,
        "depth_pct0005": float(ob.depth_pct0005 or 0.0),
        "depth_pct0010": float(ob.depth_pct0010 or 0.0),
        "depth_pct0025": float(ob.depth_pct0025 or 0.0),
    }


def momentum_category(rsi: float, roc10: float, roc30: float, roc60: float) -> Dict[str, float]:
    return {"rsi": rsi, "roc_10": roc10, "roc_30": roc30, "roc_60": roc60}


def trend_category(
    price: float,
    ema9: float,
    ema21: float,
    ema50: float,
    adx: float,
    vwap: float,
) -> Dict[str, float]:
    ema9_pct = (ema9 - ema21) / ema21 if ema21 else 0.0
    ema21_pct = (ema21 - ema50) / ema50 if ema50 else 0.0
    price_vs_ema50 = (price - ema50) / ema50 if ema50 else 0.0
    price_vs_vwap = (price - vwap) / vwap if vwap else 0.0
    if ema9 > ema21 > ema50:
        alignment = 1.0
    elif ema9 < ema21 < ema50:
        alignment = -1.0
    else:
        alignment = 0.0
    return {
        "ema9": ema9,
        "ema21": ema21,
        "ema50": ema50,
        "ema9_vs_ema21_pct": ema9_pct,
        "ema21_vs_ema50_pct": ema21_pct,
        "price_vs_ema50_pct": price_vs_ema50,
        "price_vs_vwap_pct": price_vs_vwap,
        "adx": adx,
        "trend_alignment": alignment,
        "adx_strength": min(1.0, adx / 40.0),
    }


def social_category(mention_zscore: float, news_count: int) -> Dict[str, float]:
    return {
        "mention_zscore": mention_zscore,
        "news_count": float(news_count),
        "social_score": 0.0,
    }


def news_category(news: Optional[NewsEvent], now_ms: int, ttl_ms: int) -> Dict[str, float]:
    if news is None:
        return {
            "news_sentiment": 0.0,
            "news_impact_weight": 0.0,
            "news_age_ms": 0.0,
            "news_ttl_ms": float(ttl_ms),
            "news_relevance": 0.0,
        }
    return {
        "news_sentiment": float(news.sentiment),
        "news_impact_weight": _IMPACT_WEIGHT.get(news.impact, 0.3),
        "news_age_ms": float(max(0, news.age_ms(now_ms))),
        "news_ttl_ms": float(ttl_ms),
        "news_relevance": float(news.relevance),
    }


def regime_category(regime: Regime, adx: float, atr_pct: float) -> Dict[str, float]:
    flags = {
        "is_trending": 1.0 if regime in (Regime.TRENDING_UP, Regime.TRENDING_DOWN) else 0.0,
        "is_range": 1.0 if regime is Regime.RANGE else 0.0,
        "is_breakout": 1.0 if regime is Regime.BREAKOUT else 0.0,
        "is_extreme": 1.0 if regime is Regime.EXTREME else 0.0,
        "is_high_vol": 1.0 if regime is Regime.HIGH_VOLATILITY else 0.0,
        "is_low_vol": 1.0 if regime is Regime.LOW_VOLATILITY else 0.0,
        "is_unknown": 1.0 if regime is Regime.UNKNOWN else 0.0,
    }
    return {
        "regime": float(regime.value),
        "regime_name": regime.name.lower(),
        "adx": adx,
        "atr_pct": atr_pct,
        **flags,
    }


def _boll_position(price: float, upper: float, mid: float, lower: float) -> float:
    width = upper - lower
    if width <= 0:
        return 0.0
    return (price - mid) / width