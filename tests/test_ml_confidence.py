"""FASE 4 — probability → ml_score mapping (confidence module).

Covers: score bounds and direction, edge/confidence/uncertainty semantics,
normalization of raw probabilities, and determinism of the pure function.
"""

from __future__ import annotations

import math

import pytest

from crypto_scalper.ml.confidence import confidence_from_proba


class TestConfidenceFromProba:
    def test_all_up_is_max_bullish(self):
        c = confidence_from_proba(p_up=1.0, p_down=0.0, p_neutral=0.0)
        assert c.ml_score == pytest.approx(100.0)
        assert c.edge == pytest.approx(1.0)
        assert c.confidence == pytest.approx(1.0)
        assert c.uncertainty == pytest.approx(0.0)
        assert c.dominant_class == 2

    def test_all_down_is_max_bearish(self):
        c = confidence_from_proba(p_up=0.0, p_down=1.0, p_neutral=0.0)
        assert c.ml_score == pytest.approx(0.0)
        assert c.edge == pytest.approx(-1.0)
        assert c.dominant_class == 0

    def test_tie_is_neutral_fifty(self):
        c = confidence_from_proba(p_up=0.5, p_down=0.5, p_neutral=0.0)
        assert c.ml_score == pytest.approx(50.0)
        assert c.edge == pytest.approx(0.0)
        assert c.confidence == 0.0
        assert c.dominant_class == 0  # argmax returns the first maximum (down)

    def test_mild_up_scores_above_fifty(self):
        c = confidence_from_proba(p_up=0.55, p_down=0.35, p_neutral=0.10)
        assert 50.0 < c.ml_score <= 100.0
        assert c.edge == pytest.approx(0.2)

    def test_probabilities_are_normalized(self):
        c = confidence_from_proba(p_up=0.9, p_down=0.10, p_neutral=0.10)
        # after normalization p_up=0.818, p_down=0.091 -> edge = 0.727
        assert c.ml_score == pytest.approx(50.0 + 50.0 * (0.9 - 0.1) / 1.1)
        assert c.edge == pytest.approx((0.9 - 0.1) / 1.1)

    def test_uncertainty_max_for_uniform(self):
        c = confidence_from_proba(p_up=1 / 3, p_down=1 / 3, p_neutral=1 / 3)
        assert c.uncertainty == pytest.approx(1.0)

    def test_out_of_range_clipped(self):
        c = confidence_from_proba(p_up=-0.5, p_down=2.0, p_neutral=0.0)
        # clipped to [0,1] then normalized -> down dominates
        assert c.ml_score < 50.0

    def test_deterministic(self):
        a = confidence_from_proba(0.6, 0.3, 0.1)
        b = confidence_from_proba(0.6, 0.3, 0.1)
        assert a == b


class TestScoreScale:
    def test_score_maps_units_monotonically(self):
        scores = [
            confidence_from_proba(p_up=x, p_down=0.1, p_neutral=0.9 - x).ml_score
            for x in (0.2, 0.5, 0.7)
        ]
        assert scores[0] <= scores[1] <= scores[2]