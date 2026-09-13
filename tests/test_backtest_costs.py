"""FASE 8 — CostModel / CostBreakdown / expected-edge gate."""

from __future__ import annotations

import pytest

from crypto_scalper.config.backtest import CostModelConfig
from crypto_scalper.core.enums import SignalType
from crypto_scalper.core.models import FeatureSnapshot, Signal
from crypto_scalper.risk.cost_model import (
    CostModel,
    estimate_p_win,
    p_win_from_score,
)


def _snapshot(ml: dict) -> FeatureSnapshot:
    return FeatureSnapshot(
        symbol="BTCUSDT", timestamp_ms=1, price=100.0, vwap=100.0,
        rsi=50.0, atr=1.0, atr_pct=0.01, ema9=100.0, ema21=100.0,
        ema50=100.0, adx=20.0, boll_upper=110.0, boll_mid=100.0,
        boll_lower=90.0, roc=0.0, volume_zscore=0.0, relative_volume=1.0,
        buy_volume=1.0, sell_volume=1.0, trade_count=10, avg_trade_size=0.1,
        aggressive_volume=0.2, order_book_imbalance=0.0, microprice=100.0,
        spread=0.0, spread_pct=0.0, bid_depth=0.0, ask_depth=0.0,
        news_sentiment=0.0, news_impact="low", news_age_ms=0,
        mention_zscore=0.0, regime="range", features_by_category={"ml": ml},
    )


def _signal(score, side="LONG"):
    return Signal(
        symbol="BTCUSDT", timestamp_ms=1,
        signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
        score=score, regime="range", eligible=True,
    )


class TestCostBreakdown:
    def test_fees_round_trip_entry_exit(self):
        b = CostModel(CostModelConfig(
            fee_pct=0.001, spread_pct=0.0, slippage_pct=0.0,
            latency_pct=0.0, funding_pct_per_8h=0.0,
        )).round_trip(entry_notional=100.0, exit_notional=200.0)
        assert b.fees == pytest.approx(0.3)          # (100+200) * 0.001
        assert b.slippage_attributed == 0.0          # já está embebido en fills

    def test_funding_scales_with_holding(self):
        b = CostModel(CostModelConfig(
            funding_pct_per_8h=0.0008, fee_pct=0.0, spread_pct=0.0,
            slippage_pct=0.0, latency_pct=0.0,
        )).round_trip(entry_notional=100.0, exit_notional=100.0, holding_hours=16.0)
        assert b.funding == pytest.approx(0.16)

    def test_attributed_total_matches_sum(self):
        b = CostModel(CostModelConfig(
            fee_pct=0.001, spread_pct=0.0005, slippage_pct=0.002,
            latency_pct=0.00025, funding_pct_per_8h=0.0001,
        )).round_trip(entry_notional=1000.0, exit_notional=1000.0)
        assert b.attributed_total == pytest.approx(
            b.fees + b.spread + b.latency + b.funding
        )


class TestEdgeGate:
    def test_strong_positive_edge_passes(self):
        ok, edge, cost = CostModel(CostModelConfig(
            minimum_required_edge_pct=0.001, fee_pct=0.0, spread_pct=0.0,
            slippage_pct=0.0, latency_pct=0.0, funding_pct_per_8h=0.0,
        )).edge_pass(
            p_win=0.7, rr_ratio=2.0, risk_amount=100.0, notional=1000.0,
            entry_notional=1000.0,
        )
        assert ok
        assert edge == pytest.approx(0.11)   # (0.7*2 - 0.3) * 100/1000
        assert cost == pytest.approx(0.0)

    def test_negative_edge_fails(self):
        ok, edge, _cost = CostModel(CostModelConfig(
            minimum_required_edge_pct=0.0, fee_pct=0.0, spread_pct=0.0,
            slippage_pct=0.0, latency_pct=0.0, funding_pct_per_8h=0.0,
        )).edge_pass(
            p_win=0.3, rr_ratio=2.0, risk_amount=100.0, notional=1000.0,
            entry_notional=1000.0,
        )
        assert not ok
        assert edge == pytest.approx(-0.01)

    def test_costs_are_counted(self):
        ok, _edge, cost = CostModel(CostModelConfig(
            minimum_required_edge_pct=0.0, fee_pct=0.005, spread_pct=0.001,
            slippage_pct=0.001, latency_pct=0.001, funding_pct_per_8h=0.0,
        )).edge_pass(
            p_win=0.53, rr_ratio=2.0, risk_amount=100.0, notional=1000.0,
            entry_notional=1000.0,
        )
        # EV = 0.53*2 - 0.47 = 0.59R → 5.9% edge over 1000 notional.
        # Costo: fees 2 legs (2*0.005) + spread 2 legs (2*0.001) + latency
        # 2 legs (2*0.001) + slippage round-trip (2*0.001) = 0.016 rel. a factor.
        assert cost == pytest.approx(0.016)
        assert ok


class TestPWinEstimate:
    def test_ml_probability_wins_over_score(self):
        snap = _snapshot({"p_up": 0.9, "p_down": 0.02, "p_neutral": 0.08})
        assert estimate_p_win(_signal(70.0, "LONG"), snap, "BUY") == pytest.approx(0.9)
        assert estimate_p_win(_signal(70.0, "SHORT"), snap, "SELL") == pytest.approx(0.02)

    def test_score_fallback_long_short(self):
        assert p_win_from_score(70.0, "BUY") == pytest.approx(0.6)
        assert p_win_from_score(70.0, "SELL") == pytest.approx(0.4)
        assert p_win_from_score(50.0, "BUY") == pytest.approx(0.5)

    def test_score_fallback_clamped(self):
        assert p_win_from_score(0.0, "BUY") == pytest.approx(0.25)
        assert p_win_from_score(100.0, "BUY") == pytest.approx(0.75)

    def test_no_ml_falls_back_to_score(self):
        snap = _snapshot({})
        assert estimate_p_win(_signal(60.0, "LONG"), snap, "BUY") == pytest.approx(0.55)