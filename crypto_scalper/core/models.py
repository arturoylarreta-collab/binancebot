"""Domain models.

Typed containers that flow between layers. They must be timestamped and
carry no exchange-specific logic (raw parsing lives in market_data).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from crypto_scalper.core.enums import AggressorSide, Impact, Regime, SignalType


def now_utc_ms() -> int:
    return int(time.time() * 1000)


# ── Market data ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AggTrade:
    symbol: str
    event_time_ms: int        # raw exchange trade/first-trade event time
    trade_id: int
    price: float
    quantity: float
    aggressor: AggressorSide = AggressorSide.UNKNOWN
    ingest_mono_ms: int = 0   # local monotonic time when ingested (latency)


@dataclass(frozen=True)
class DiffDepthEvent:
    symbol: str
    event_time_ms: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    bids: Tuple[Tuple[float, float], ...]     # (price, qty)
    asks: Tuple[Tuple[float, float], ...]
    ingest_mono_ms: int = 0


@dataclass(frozen=True)
class Candle:
    symbol: str
    ts_ms: int                # open time (bucket aligned)
    interval_s: int
    open: float
    high: float
    low: float
    close: float
    volume: float             # base volume
    quote_volume: float
    trade_count: int
    completed: bool = False


@dataclass
class OrderBookLevel:
    price: float
    quantity: float


@dataclass
class OrderBookMetrics:
    symbol: str
    ts_ms: int
    best_bid: float
    best_ask: float
    spread: float
    spread_pct: float
    bid_depth: float
    ask_depth: float
    imbalance: float
    microprice: float
    levels: int = 0
    depth_pct0005: Optional[float] = None      # sum within ±0.05% of mid
    depth_pct0010: Optional[float] = None      # sum within ±0.10% of mid
    depth_pct0025: Optional[float] = None      # sum within ±0.25% of mid


# ── News / sentiment ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NewsEvent:
    symbol: Optional[str]
    timestamp_ms: int
    headline: str
    source: str
    sentiment: float        # in [-1, +1]
    impact: Impact
    relevance: float        # [0, 1]
    mention_count: int = 0
    language: str = "en"
    raw: Dict[str, Any] = field(default_factory=dict)

    def age_ms(self, reference_ms: Optional[int] = None) -> int:
        return max(0, (reference_ms or now_utc_ms()) - self.timestamp_ms)


# ── Features ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FeatureSnapshot:
    """Normalized, timestamped feature vector for one symbol at one instant.

    Produced by the Feature Engine (FASE 2/3) and consumed by Signal/ML/Risk.
    No information from the future may ever be included.
    """

    symbol: str
    timestamp_ms: int
    price: float
    vwap: float
    rsi: float
    atr: float
    atr_pct: float
    ema9: float
    ema21: float
    ema50: float
    adx: float
    boll_upper: float
    boll_mid: float
    boll_lower: float
    roc: float
    volume_zscore: float
    relative_volume: float
    buy_volume: float
    sell_volume: float
    trade_count: int
    avg_trade_size: float
    aggressive_volume: float
    order_book_imbalance: float
    microprice: float
    spread: float
    spread_pct: float
    bid_depth: float
    ask_depth: float
    news_sentiment: float
    news_impact: str
    news_age_ms: int
    mention_zscore: float
    regime: str
    extra: Dict[str, Any] = field(default_factory=dict)
    features_by_category: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for f in self.__dataclass_fields__:
            v = getattr(self, f)
            if isinstance(v, (int, float, str, bool)) or v is None:
                d[f] = v
            elif isinstance(v, dict):
                d[f] = {k: vv for k, vv in v.items() if isinstance(vv, (int, float, str, bool))}
        return d


@dataclass(frozen=True)
class SignalComponent:
    """One dimension of a Signal Score (0-100, higher = more bullish)."""

    name: str
    score: float
    weight: float
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Signal:
    """A non-binary signal: score, direction, per-dimension breakdown.

    Produced by the Signal Engine (FASE 3) from a FeatureSnapshot. Never
    generates orders on its own; Risk Engine (FASE 5) keeps final authority.
    """

    symbol: str
    timestamp_ms: int
    signal_type: SignalType
    score: float
    regime: str
    eligible: bool
    reason: str = ""
    components: Dict[str, SignalComponent] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp_ms": self.timestamp_ms,
            "signal_type": self.signal_type.name,
            "score": self.score,
            "regime": self.regime,
            "eligible": self.eligible,
            "reason": self.reason,
            "components": {
                name: {"score": c.score, "weight": c.weight, "detail": c.detail}
                for name, c in self.components.items()
            },
        }


@dataclass(frozen=True)
class FeatureRequest:
    symbol: str
    ts_ms: int
    requested_by: str


# ── Orders (used from FASE 6 onward) ───────────────────────────────────────────


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    time_in_force: str = "GTC"
    requested_ts_ms: int = 0


# ── Risk ───────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RiskDecision:
    symbol: str
    ts_ms: int
    verdict: str            # RiskVerdict.value
    reason: str = ""
    verifier: str = "risk_engine"
    details: Dict[str, Any] = field(default_factory=dict)


# ── Latency ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LatencyRecord:
    symbol: str
    event_ts_ms: int
    ingest_mono_ms: int
    processed_mono_ms: int
    feature_mono_ms: int