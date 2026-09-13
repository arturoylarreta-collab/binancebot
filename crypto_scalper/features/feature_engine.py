"""FusionEngine + Feature Engine that produce FeatureSnapshot objects.

Pipeline: Market State + latest relevant news + social/trending state.
The news item is only fused if fresh (TTL enforced by NewsStore); old news
is never mixed into a new decision snapshot.

No future information enters here: every input was available at now_ms.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import numpy as np

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.core.models import FeatureSnapshot, OrderBookMetrics
from crypto_scalper.core.enums import Impact, Regime
from crypto_scalper.core.exceptions import MarketDataNotReady
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features import price_features, technicals as t, volatility, volume_features
from crypto_scalper.features.regime import MarketRegime
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

log = logging.getLogger(__name__)


class FeatureEngine:
    def __init__(
        self,
        config: FeatureConfig,
        news_store: NewsStore,
        metrics: Metrics,
        regime_classifier: Optional[MarketRegime] = None,
    ) -> None:
        self._config = config
        self._news = news_store
        self._metrics = metrics
        self._regime = regime_classifier or MarketRegime()

    def compute(self, state: SymbolState, now_ms: Optional[int] = None) -> FeatureSnapshot:
        now_ms = now_ms or int(time.time() * 1000)
        series = state.candles.series()
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
        boll_lower_f = _nan_to(boll_lower, price)

        vol_features = volatility.volatility_features(high, low, close)
        atr, atr_pct = vol_features["atr"], vol_features["atr_pct"]

        vwap = t.session_vwap(series.ts, typical, series.volume)
        vwap_f = _nan_to(vwap, price)

        vol = volume_features.volume_features(state.trades, now_ms)

        ob: Optional[OrderBookMetrics] = None
        if state.orderbook.has_snapshot:
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

        news = self._news.latest(state.symbol, now_ms=now_ms)
        news_sentiment = news.sentiment if news else 0.0
        news_impact = news.impact.name.lower() if news else Impact.LOW.name.lower()
        news_age = news.age_ms(now_ms) if news else 0
        mention_zscore = self._news.mention_zscore(state.symbol)

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

        extra: Dict[str, float] = {
            "ob_levels": float(ob.levels) if ob else 0.0,
            "price_features": price_features.price_features(close, high, low),
            "realized_volatility": vol_features["realized_volatility"],
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
            boll_mid=_nan_to(boll_mid, price),
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
        )


def _fallback(value: float, fallback: float) -> float:
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return fallback
    return float(value)


def _nan_to(value: float, fallback: float) -> float:
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return fallback
    return float(value)