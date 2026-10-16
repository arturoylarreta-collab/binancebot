"""FusionEngine + Feature Engine that produce FeatureSnapshot objects.

Pipeline: Market State + latest relevant news + social/trending state.
The news item is only fused if fresh (TTL enforced by NewsStore); old news
is never mixed into a new decision snapshot.

FASE 3 additionally emits every feature grouped by category (PRICE, VOLUME,
VOLATILITY, ORDER FLOW, ORDER BOOK, MOMENTUM, TREND, SOCIAL, NEWS, MARKET
REGIME), all timestamped with the same `now_ms`: no future information is
available at that instant.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Dict, Optional

import numpy as np

from crypto_scalper.core import clock

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.core.models import FeatureSnapshot, OrderBookMetrics
from crypto_scalper.core.enums import Impact, Regime
from crypto_scalper.core.exceptions import MarketDataNotReady
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features import categories, price_features, technicals as t, volatility, volume_features
from crypto_scalper.features.regime import MarketRegime
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

if TYPE_CHECKING:
    from crypto_scalper.ml.predictor import MLPredictor

log = logging.getLogger(__name__)


class FeatureEngine:
    def __init__(
        self,
        config: FeatureConfig,
        news_store: NewsStore,
        metrics: Metrics,
        regime_classifier: Optional[MarketRegime] = None,
        predictor: Optional["MLPredictor"] = None,
    ) -> None:
        self._config = config
        self._news = news_store
        self._metrics = metrics
        self._regime = regime_classifier or MarketRegime()
        self._last_obi: Dict[str, float] = {}
        self._predictor = predictor
        # Indicators only need a bounded window; recomputing over the whole
        # 2000-candle history every second is the dominant CPU cost.
        self._lookback = max(120, int(getattr(config, "lookback_candles", 900)))

    def compute(self, state: SymbolState, now_ms: Optional[int] = None) -> FeatureSnapshot:
        now_ms = now_ms or clock.now_ms()
        series = _tail(state.candles.series(), self._lookback)
        if len(series.close) < 2 or np.isnan(series.close[-1]):
            raise MarketDataNotReady(f"{state.symbol}: not enough candle data")

        price = state.latest_price if state.latest_price else float(series.close[-1])
        close, high, low = series.close, series.high, series.low
        typical = t.typical_price(high, low, close)

        ema9 = _fallback(t.ema(close, 9), price)
        ema21 = _fallback(t.ema(close, 21), price)
        ema50 = _fallback(t.ema(close, 50), price)
        rsi = _fallback(t.rsi(close, 14), 50.0)
        adx = _fallback(t.adx(high, low, close, 14), 0.0)
        boll_upper, boll_mid, boll_lower = t.bollinger(close, 20, 2.0)
        boll_upper_f = _nan_to(boll_upper, price)
        boll_mid_f = _nan_to(boll_mid, price)
        boll_lower_f = _nan_to(boll_lower, price)

        vol_features = volatility.volatility_features(high, low, close)
        atr, atr_pct = vol_features["atr"], vol_features["atr_pct"]
        realized_vol = vol_features["realized_volatility"]

        vwap = t.session_vwap(series.ts, typical, series.volume)
        vwap_f = _nan_to(vwap, price)

        pf = price_features.price_features(close, high, low)
        roc10 = pf.get("roc_10", 0.0)
        if roc10 is None or (isinstance(roc10, float) and (np.isnan(roc10) or np.isinf(roc10))):
            roc10 = 0.0
        roc30 = _nan_to(t.roc(close, 30), 0.0)
        roc60 = _nan_to(t.roc(close, 60), 0.0)

        vol = volume_features.volume_features(state.trades, now_ms)

        ob: Optional[OrderBookMetrics] = None
        if state.orderbook.is_synced:
            try:
                ob = state.orderbook.metrics(self._config.depth_pct_buckets)
            except Exception:  # noqa: BLE001 - OBI is optional for the snapshot
                ob = None
        obi = ob.imbalance if ob else 0.0
        microprice = ob.microprice if ob else price
        spread = ob.spread if ob else 0.0
        spread_pct = ob.spread_pct if ob else 0.0
        bid_depth = ob.bid_depth if ob else 0.0
        ask_depth = ob.ask_depth if ob else 0.0

        obi_delta = 0.0
        if ob is not None:
            prev = self._last_obi.get(state.symbol)
            obi_delta = (obi - prev) if prev is not None else 0.0
            self._last_obi[state.symbol] = obi

        news = self._news.latest(state.symbol, now_ms=now_ms)
        news_sentiment = news.sentiment if news else 0.0
        news_impact = news.impact.name.lower() if news else Impact.LOW.name.lower()
        news_age = news.age_ms(now_ms) if news else 0
        mention_zscore = self._news.mention_zscore(state.symbol)
        news_count = self._news.recent_count(state.symbol, now_ms=now_ms)

        regime = self._regime.classify(
            price=price,
            ema9=ema9,
            ema21=ema21,
            ema50=ema50,
            adx=adx,
            atr_pct=atr_pct,
            boll_upper=boll_upper_f,
            boll_lower=boll_lower_f,
        )

        features_by_category = categories.build_all(
            state=state,
            price=price,
            close=close,
            high=high,
            low=low,
            vwap=vwap_f,
            ema9=ema9,
            ema21=ema21,
            ema50=ema50,
            rsi=rsi,
            adx=adx,
            roc10=float(roc10),
            roc30=roc30,
            roc60=roc60,
            boll_upper=boll_upper_f,
            boll_mid=boll_mid_f,
            boll_lower=boll_lower_f,
            atr=atr,
            atr_pct=atr_pct,
            realized_vol=realized_vol,
            vol=vol,
            ob=ob,
            obi_delta=obi_delta,
            news=news,
            mention_zscore=mention_zscore,
            news_count=news_count,
            now_ms=now_ms,
            ttl_ms=self._news.ttl_ms,
            regime=regime,
        )

        if self._predictor is not None:
            features_by_category["ml"] = dict(
                self._predictor.category(features_by_category)
            )

        extra: Dict[str, float] = {
            "ob_levels": float(ob.levels) if ob else 0.0,
            "price_features": pf,
            "realized_volatility": realized_vol,
            "volume_acceleration": vol.get("volume_acceleration", 0.0),
            "buy_ratio_30s": vol.get("buy_ratio_30s", 0.0),
            "depth_pct0005": float(ob.depth_pct0005 or 0.0) if ob else 0.0,
            "depth_pct0010": float(ob.depth_pct0010 or 0.0) if ob else 0.0,
            "depth_pct0025": float(ob.depth_pct0025 or 0.0) if ob else 0.0,
            "state_ready": 1.0 if state.is_ready() else 0.0,
        }

        self._metrics.set_gauge(f"feature.{state.symbol}.regime", float(regime.value))
        return FeatureSnapshot(
            symbol=state.symbol,
            timestamp_ms=now_ms,
            price=price,
            vwap=vwap_f,
            rsi=rsi,
            atr=atr,
            atr_pct=atr_pct,
            ema9=ema9,
            ema21=ema21,
            ema50=ema50,
            adx=adx,
            boll_upper=boll_upper_f,
            boll_mid=boll_mid_f,
            boll_lower=boll_lower_f,
            roc=_fallback(t.roc(close, 10), 0.0),
            volume_zscore=vol.get("volume_zscore", 0.0),
            relative_volume=vol.get("relative_volume", 0.0),
            buy_volume=vol.get("buy_volume", 0.0),
            sell_volume=vol.get("sell_volume", 0.0),
            trade_count=int(vol.get("trade_count_30s", 0)),
            avg_trade_size=vol.get("avg_trade_size_30s", 0.0),
            aggressive_volume=vol.get("aggressive_volume_30s", 0.0),
            order_book_imbalance=obi,
            microprice=microprice,
            spread=spread,
            spread_pct=spread_pct,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            news_sentiment=news_sentiment,
            news_impact=news_impact,
            news_age_ms=news_age,
            mention_zscore=mention_zscore,
            regime=regime.name.lower(),
            extra=extra,
            features_by_category=features_by_category,
        )


def _fallback(value: float, fallback: float) -> float:
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return fallback
    return float(value)


def _nan_to(value: float, fallback: float) -> float:
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return fallback
    return float(value)


def _tail(series, n: int):
    """Return a CandleSeries view restricted to the last ``n`` candles."""
    if len(series.close) <= n:
        return series
    import dataclasses
    return dataclasses.replace(
        series, **{f.name: getattr(series, f.name)[-n:] for f in dataclasses.fields(series)}
    )
