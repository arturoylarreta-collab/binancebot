"""MLPredictor — trained artifact serializable with joblib (FASE 4).

Holds the calibrated classifier, the ValueSchema learned during training
(the deterministic column mapping), and the labeler parameters. Exposes:

  - `predict(features_by_category)` → MLPrediction  (full provenance)
  - `category(features_by_category)` → Dict[str, float]
    which is exactly what gets injected into
    `FeatureSnapshot.features_by_category["ml"]` and consumed by the `ml`
    scorer of the Signal Engine.

No pandas; hot-path is a single numpy row + sklearn predict_proba call.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, Dict, Tuple

import joblib
import numpy as np

from crypto_scalper.ml.classifier import predict_proba as _predict_proba
from crypto_scalper.ml.confidence import confidence_from_proba
from crypto_scalper.ml.features import CLASS_NAMES, ValueSchema


@dataclasses.dataclass(frozen=True)
class MLPrediction:
    p_up: float
    p_down: float
    p_neutral: float
    ml_score: float
    confidence: float
    uncertainty: float
    edge: float
    dominant_class_name: str
    model_version: str


class MLPredictor:
    """Callable predictor: `(cats) → Dict[str, float]` (the ml category)."""

    def __init__(
        self,
        model: Any,
        schema: ValueSchema,
        *,
        horizon_s: int,
        threshold: float,
        model_version: str,
        model_name: str = "",
        score_k: float = 50.0,
    ) -> None:
        self._model = model
        self._schema = schema
        self._horizon_s = int(horizon_s)
        self._threshold = float(threshold)
        self._model_version = str(model_version)
        self._model_name = str(model_name)
        self._score_k = float(score_k)

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def horizon_s(self) -> int:
        return self._horizon_s

    @property
    def threshold(self) -> float:
        return self._threshold

    @property
    def schema(self) -> ValueSchema:
        return self._schema

    def _vectorize(
        self, features_by_category: Dict[str, Dict[str, float]]
    ) -> np.ndarray:
        return self._schema.to_row(features_by_category)

    def predict(
        self, features_by_category: Dict[str, Dict[str, float]]
    ) -> MLPrediction:
        row = self._vectorize(features_by_category)
        proba = _predict_proba(self._model, row.reshape(1, -1))
        p_down, p_neutral, p_up = proba[0].tolist()
        c = confidence_from_proba(p_up, p_down, p_neutral, score_k=self._score_k)
        dominant_name = CLASS_NAMES[c.dominant_class]
        return MLPrediction(
            p_up=float(p_up),
            p_down=float(p_down),
            p_neutral=float(p_neutral),
            ml_score=float(c.ml_score),
            confidence=float(c.confidence),
            uncertainty=float(c.uncertainty),
            edge=float(c.edge),
            dominant_class_name=str(dominant_name),
            model_version=self._model_version,
        )

    def category(
        self, features_by_category: Dict[str, Dict[str, float]]
    ) -> Dict[str, float]:
        """The dict injected into features_by_category['ml']."""
        pred = self.predict(features_by_category)
        return {
            "p_up": float(pred.p_up),
            "p_down": float(pred.p_down),
            "p_neutral": float(pred.p_neutral),
            "ml_score": float(pred.ml_score),
            "confidence": float(pred.confidence),
            "uncertainty": float(pred.uncertainty),
            "edge": float(pred.edge),
        }

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, str(path), compress=3)

    @classmethod
    def load(cls, path: str | Path) -> "MLPredictor":
        obj = joblib.load(str(path))
        if not isinstance(obj, MLPredictor):
            raise TypeError(f"expected MLPredictor, got {type(obj).__name__}")
        return obj