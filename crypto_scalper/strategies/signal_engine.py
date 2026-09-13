"""Signal Engine (FASE 3).

Consumes a FeatureSnapshot and produces a non-binary Signal Score in
[0, 100] decomposed per dimension (trend, momentum, volume, order_book,
volatility, price_structure, news, ml). Weights come from StrategyConfig;
they are explicitly NOT claimed optimal (to be validated in FASE 8).

The engine only reads features already timestamped at decision time (no
look-ahead), never places orders, and never consults RiskConfig (FASE 5).
Regime gating: a strategy may refuse to act in a regime via
`allowed_regimes` on StrategyConfig.
"""

from __future__ import annotations

from typing import Callable, Dict

from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.core.enums import SignalType
from crypto_scalper.core.models import FeatureSnapshot, Signal, SignalComponent

_SCORERS: Dict[str, Callable[[Dict[str, float]], float]] = {}


def _register(name: str) -> Callable:
    def deco(fn: Callable[[Dict[str, float]], float]) -> Callable:
        _SCORERS[name] = fn
        return fn

    return deco


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


@_register("trend")
def _score_trend(cat: Dict[str, float]) -> float:
    alignment = float(cat.get("trend_alignment", 0.0))
    strength = float(cat.get("adx_strength", 0.0))
    return _clip(50.0 + alignment * strength * 50.0)


@_register("momentum")
def _score_momentum(cat: Dict[str, float]) -> float:
    rsi = float(cat.get("rsi", 50.0))
    roc10 = float(cat.get("roc_10", 0.0))
    roc30 = float(cat.get("roc_30", 0.0))
    rsi_c = (rsi - 50.0) / 50.0
    n10 = _clip(roc10 * 100.0 / 0.5, -1.0, 1.0)
    n30 = _clip(roc30 * 100.0 / 1.0, -1.0, 1.0)
    mom = 0.6 * rsi_c + 0.25 * n10 + 0.15 * n30
    return _clip(50.0 + mom * 50.0)


@_register("volume")
def _score_volume(cat: Dict[str, float]) -> float:
    vol_z = float(cat.get("volume_zscore", 0.0))
    rvol = float(cat.get("relative_volume", 0.0))
    buy_ratio = float(cat.get("buy_ratio_30s", 0.5))
    rvol_c = _clip(rvol / 5.0, 0.0, 1.0)
    dir_c = (buy_ratio - 0.5) * 2.0
    z_c = _clip(vol_z / 5.0, -1.0, 1.0)
    return _clip(50.0 + dir_c * rvol_c * 40.0 + z_c * 10.0)


@_register("order_book")
def _score_order_book(cat: Dict[str, float]) -> float:
    obi = float(cat.get("obi", 0.0))
    obi_delta = float(cat.get("obi_delta", 0.0))
    spread_pct = float(cat.get("spread_pct", 0.001))
    obi_c = _clip(obi / 0.4, -1.0, 1.0) * 40.0
    spread_pen = _clip(spread_pct / 0.0015, 0.0, 1.0) * 15.0
    delta_c = _clip(obi_delta / 0.1, -1.0, 1.0) * 10.0
    return _clip(50.0 + obi_c - spread_pen + delta_c, 0.0, 100.0)


@_register("volatility")
def _score_volatility(cat: Dict[str, float]) -> float:
    v = float(cat.get("atr_pct", 0.0))
    if v <= 0.0002:
        return 25.0
    if v >= 0.006:
        return 30.0
    if v < 0.0012:
        return _clip(40.0 + (v - 0.0002) / 0.0010 * 40.0)
    return _clip(80.0 - (v - 0.0012) / 0.0048 * 50.0)


@_register("price_structure")
def _score_price_structure(cat: Dict[str, float]) -> float:
    vwap_d = float(cat.get("price_vs_vwap_pct", 0.0))
    boll = float(cat.get("boll_position", 0.0))
    ret60 = float(cat.get("return_60s", 0.0))
    vwap_c = _clip(vwap_d / 0.0015, -1.0, 1.0) * 30.0
    boll_c = _clip(boll, -1.0, 1.0) * 20.0
    ret_c = _clip(ret60 / 0.002, -1.0, 1.0) * 10.0
    return _clip(50.0 + vwap_c + boll_c + ret_c, 0.0, 100.0)


@_register("news")
def _score_news(cat: Dict[str, float]) -> float:
    age = float(cat.get("news_age_ms", 0.0))
    ttl = float(cat.get("news_ttl_ms", 0.0))
    relevance = float(cat.get("news_relevance", 0.0))
    sentiment = float(cat.get("news_sentiment", 0.0))
    impact_w = float(cat.get("news_impact_weight", 0.0))
    if ttl > 0 and age <= ttl and relevance > 0:
        freshness = 1.0 - age / ttl
        strength = impact_w * freshness * min(1.0, relevance)
        return _clip(50.0 + sentiment * strength * 50.0)
    return 50.0


@_register("ml")
def _score_ml(cat: Dict[str, float]) -> float:
    """FASE 4: reads the scaled probability produced by the ML predictor.

    Without a predictor wired into the Feature Engine there is no `ml`
    category and the dimension stays neutral (50.0). With one, `ml_score`
    is in [0, 100] (50 = neutral, higher = more bullish) and enters the
    weighted Signal Score like any other dimension.
    """
    return _clip(float(cat.get("ml_score", 50.0)))


class SignalEngine:
    def __init__(self, config: StrategyConfig) -> None:
        self._config = config

    @property
    def weights(self) -> Dict[str, float]:
        return dict(self._config.signal_weights)

    def evaluate(self, snapshot: FeatureSnapshot) -> Signal:
        cats = snapshot.features_by_category
        _CLAMP = _clip
        components: Dict[str, SignalComponent] = {}
        for name, scorer in _SCORERS.items():
            weight = self._config.signal_weights.get(name, 0.0)
            if weight <= 0:
                continue
            source = _SOURCE_CATEGORIES.get(name, name)
            detail = cats.get(source, {})
            score = _CLAMP(float(scorer(detail)))
            components[name] = SignalComponent(
                name=name,
                score=score,
                weight=weight,
                detail=detail,
            )
        score = sum(c.score * c.weight for c in components.values())
        gated, reason = self._gating(snapshot)
        signal_type = SignalType.FLAT
        if not gated:
            if score >= self._config.long_threshold:
                signal_type = SignalType.LONG
            elif score <= self._config.short_threshold:
                signal_type = SignalType.SHORT
        return Signal(
            symbol=snapshot.symbol,
            timestamp_ms=snapshot.timestamp_ms,
            signal_type=signal_type,
            score=round(score, 6),
            regime=snapshot.regime,
            eligible=not gated,
            reason=reason,
            components=components,
        )

    def _gating(self, snapshot: FeatureSnapshot):
        if not self._config.enabled:
            return True, "strategy_disabled"
        allowed = self._config.allowed_regimes
        if allowed and snapshot.regime not in allowed:
            return True, f"regime_blocked:{snapshot.regime}"
        return False, ""


_SOURCE_CATEGORIES = {
    "trend": "trend",
    "momentum": "momentum",
    "volume": "volume",
    "order_book": "order_book",
    "volatility": "volatility",
    "price_structure": "price",
    "news": "news",
    "ml": "ml",
}