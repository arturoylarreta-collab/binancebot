"""Probability → ml_score mapping (FASE 4).

Maps P(down), P(neutral), P(up) to:

  - ml_score  : 0–100 (50 = neutral; >50 bullish; <50 bearish)
  - edge      : signed margin between up and down
  - confidence: strength of the prediction (0 = coin-flip, 1 = certain)
  - uncertainty: normalized Shannon entropy in [0, 1]

The ml_score is read by `_score_ml` in the Signal Engine (FASE 3 interface)
and enters the weighted sum of the 0–100 Signal Score exactly like any other
dimension. ML never overrides execution; Risk Engine (FASE 5) keeps authority.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

_LOG3 = math.log(3.0)


@dataclass(frozen=True)
class ConfidenceProperties:
    ml_score: float
    edge: float
    confidence: float
    uncertainty: float
    dominant_class: int  # 0=down, 1=neutral, 2=up


def confidence_from_proba(
    p_up: float,
    p_down: float,
    p_neutral: float,
    score_k: float = 50.0,
) -> ConfidenceProperties:
    """Pure-function conversion from raw probabilities to a 0–100 score.

    Parameters
    ----------
    p_up, p_down, p_neutral : calibrated probabilities summing to ~1.0
    score_k : base score for the neutral midpoint (default 50.0)

    Returns
    -------
    ConfidenceProperties  (ml_score, edge, confidence, uncertainty, dominant_class)
    """
    p_down = float(np.clip(p_down, 0.0, 1.0))
    p_up = float(np.clip(p_up, 0.0, 1.0))
    p_neutral = float(np.clip(p_neutral, 0.0, 1.0))
    total = p_down + p_up + p_neutral
    if total > 0:
        p_down /= total
        p_up /= total
        p_neutral /= total

    edge = p_up - p_down

    ml_score = max(0.0, min(100.0, score_k + score_k * edge))
    confidence = min(1.0, abs(edge) * 2.0)

    probs = [p for p in (p_down, p_neutral, p_up) if p > 0.0]
    entropy = -sum(p * math.log(p) for p in probs) / _LOG3 if probs else 0.0
    uncertainty = max(0.0, min(1.0, entropy))

    dominant_class = int(np.argmax([p_down, p_neutral, p_up]))

    return ConfidenceProperties(
        ml_score=float(ml_score),
        edge=float(edge),
        confidence=float(confidence),
        uncertainty=float(uncertainty),
        dominant_class=dominant_class,
    )