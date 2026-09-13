"""End-to-end offline test of the Feature Engine (FusionEngine) producing a
FeatureSnapshot from a locally-built SymbolState, including news TTL."""

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.core.enums import AggressorSide, Impact
from crypto_scalper.core.exceptions import MarketDataNotReady
from crypto_scalper.core.models import AggTrade, NewsEvent
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

T0 = 1_700_000_000_000


def _seed_state(symbol="BTCUSDT", n_seconds=40) -> SymbolState:
    state = SymbolState(symbol)
    state.orderbook.apply_snapshot(
        100,
        [["99", "10"], ["98", "5"]],
        [["101", "10"], ["102", "5"]],
        ts_ms=T0,
    )
    price = 100.0
    for sec in range(n_seconds):
        for k in range(5):
            price += 0.01 if k % 2 == 0 else -0.005
            trade = AggTrade(
                symbol=symbol,
                event_time_ms=T0 + sec * 1000 + k,
                trade_id=sec * 5 + k,
                price=price,
                quantity=1.0,
                aggressor=AggressorSide.BUY if k % 2 == 0 else AggressorSide.SELL,
            )
            state.on_trade(trade)
    return state


class TestFeatureEngine:
    def _engine(self, news):
        return FeatureEngine(FeatureConfig(), news, Metrics())

    def test_snapshot_fields(self):
        engine = self._engine(NewsStore())
        snap = engine.compute(_seed_state())
        assert snap.symbol == "BTCUSDT"
        assert snap.price > 0
        assert snap.timestamp_ms >= T0
        assert snap.rsi == 50.0 or 0.0 <= snap.rsi <= 100.0
        assert snap.atr > 0
        assert snap.vwap > 0
        assert isinstance(snap.order_book_imbalance, float)
        assert isinstance(snap.spread_pct, float)
        assert snap.regime in {"range", "trending_up", "trending_down", "breakout",
                               "high_volatility", "low_volatility", "extreme", "unknown"}

    def test_news_fresh_is_fused(self):
        store = NewsStore(ttl_ms=60_000)
        state = _seed_state()
        store.put(
            NewsEvent(
                symbol="BTCUSDT", timestamp_ms=T0 + 1000,
                headline="strong accumulation", source="test",
                sentiment=0.9, impact=Impact.HIGH, relevance=1.0,
            )
        )
        snap = self._engine(store).compute(state, now_ms=T0 + 2000)
        assert snap.news_sentiment == 0.9
        assert snap.news_impact == "high"
        assert snap.news_age_ms >= 0

    def test_news_expired_is_not_fused(self):
        store = NewsStore(ttl_ms=60_000)
        state = _seed_state()
        store.put(
            NewsEvent(
                symbol="BTCUSDT", timestamp_ms=T0 - 1_000_000,
                headline="old news", source="test",
                sentiment=-0.9, impact=Impact.HIGH, relevance=1.0,
            )
        )
        snap = self._engine(store).compute(state, now_ms=T0 + 2000)
        assert snap.news_sentiment == 0.0
        assert snap.news_age_ms == 0

    def test_not_enough_candles_raises(self):
        engine = self._engine(NewsStore())
        state = SymbolState("BTCUSDT")
        state.on_trade(AggTrade("BTCUSDT", T0, 1, 100.0, 1.0))
        try:
            engine.compute(state)
        except MarketDataNotReady:
            return
        raise AssertionError("expected MarketDataNotReady")