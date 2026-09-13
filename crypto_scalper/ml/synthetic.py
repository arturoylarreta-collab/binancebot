"""Synthetic market data generator for offline ML training (FASE 4).

Generates a deterministically reproducible sequence of FeatureSnapshots by
simulating per-second price regimes, trades with aggressor side, and feeding
the real FeatureEngine so features are computed through the exact same path
as live data (no separate hand-built dicts). No network required.

Regime episodes alternate (trending_up / trending_down / range). To give the
classifier a learnable structure — and to keep every temporal window class-
diverse (so logistic regression never trains on a single class) — each
symbol starts its regime cycle at a different phase and durations follow a
fixed deterministic cycle. All symbols share the same `t0` so any walk-
forward training window spans several symbols in different regimes.

Structure: momentum / flow / EMA alignment features at time `t` correlate
with the sign of the forward return over the ongoing episode (horizon is
kept below the typical episode length).
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np

from crypto_scalper.config.settings import FeatureConfig
from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.models import AggTrade
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.monitoring.metrics import Metrics

_T0 = 1_700_000_000_000
_BASE_PRICE = 100.0
_BASE_QTY = 1.0
_TRADES_PER_SEC = 4

_DRIFT: Dict[str, Tuple[float, float]] = {
    "up":    (0.0009, 0.70),
    "down":  (-0.0009, 0.30),
    "range": (0.0000, 0.50),
}
_EPISODE_ORDER = ("up", "down", "range")
_EPISODE_DURATIONS = (30, 24, 36, 30, 24, 36, 30, 24)


class SyntheticMarket:
    """Offline-only deterministic generator.

    Parameters
    ----------
    symbols : tuple of str
    seconds_per_symbol : int
        total simulation seconds per symbol (at least ~60 recommended)
    trades_per_second : int
    seed : int
        controls price-noise / trade randomness (episodes are deterministic)
    predictor : MLPredictor or None
        when set, the generated snapshots carry the `ml` feature category
    """

    def __init__(
        self,
        symbols: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"),
        seconds_per_symbol: int = 180,
        trades_per_second: int = _TRADES_PER_SEC,
        seed: int = 42,
        predictor=None,
    ) -> None:
        self._symbols = tuple(symbols)
        self._seconds = int(seconds_per_symbol)
        self._tps = int(trades_per_second)
        self._seed = int(seed)
        self._predictor = predictor

    def _episodes(self, symbol_index: int) -> List[Tuple[int, int, str]]:
        """Deterministic (start_s, end_s, regime) schedule for one symbol."""
        phase = symbol_index % 3          # stagger regime phases across symbols
        dur_offset = (2 * symbol_index) % len(_EPISODE_DURATIONS)
        episodes: List[Tuple[int, int, str]] = []
        t = 0
        k = phase
        while t < self._seconds:
            start = t
            length = _EPISODE_DURATIONS[(k - phase + dur_offset) % len(_EPISODE_DURATIONS)]
            regime = _EPISODE_ORDER[k % len(_EPISODE_ORDER)]
            t += length
            end = min(t, self._seconds)
            if end > start:
                episodes.append((start, end, regime))
            k += 1
            if t >= self._seconds:
                break
        return episodes

    def generate(self) -> List:
        """Return a list[FeatureSnapshot] sorted globally by timestamp_ms."""
        rng = np.random.default_rng(self._seed)
        news_store = NewsStore(ttl_ms=300_000)
        engine = FeatureEngine(FeatureConfig(), news_store, Metrics(), predictor=self._predictor)
        snapshots: list = []

        for sym_i, symbol in enumerate(self._symbols):
            t0 = _T0
            state = SymbolState(symbol)
            state.orderbook.apply_snapshot(
                last_update_id=100,
                bids=[[_BASE_PRICE - 1, _BASE_QTY], [_BASE_PRICE - 2, _BASE_QTY]],
                asks=[[_BASE_PRICE + 1, _BASE_QTY], [_BASE_PRICE + 2, _BASE_QTY]],
                ts_ms=t0,
            )
            price = float(_BASE_PRICE)
            episodes = self._episodes(sym_i)
            episode_lookup: dict = {start: (end, regime) for start, end, regime in episodes}
            current_episode: tuple = episodes[0]
            current_end, current_regime = current_episode[1], current_episode[2]

            for sec in range(self._seconds):
                if sec in episode_lookup:
                    current_end, current_regime = episode_lookup[sec]

                drift, buy_frac = _DRIFT[current_regime]
                noise = float(rng.normal(0.0, 0.0004))
                price *= 1.0 + drift + noise
                price = max(price, 1.0)

                for k in range(self._tps):
                    t_ms = t0 + sec * 1000 + k
                    trade_id = sec * self._tps + k + 1
                    is_buy = rng.random() < buy_frac
                    px = price * (1.0 + float(rng.normal(0.0, 0.0001)))
                    qty = abs(float(rng.lognormal(0.0, 0.2))) * _BASE_QTY
                    aggressor = AggressorSide.BUY if is_buy else AggressorSide.SELL
                    state.on_trade(
                        AggTrade(
                            symbol=symbol,
                            event_time_ms=t_ms,
                            trade_id=trade_id,
                            price=float(px),
                            quantity=float(qty),
                            aggressor=aggressor,
                        )
                    )

                if sec >= 2:
                    snap = engine.compute(state, now_ms=t0 + sec * 1000 + self._tps - 1)
                    snapshots.append(snap)

        snapshots.sort(key=lambda s: s.timestamp_ms)
        return snapshots