"""Machine Learning layer (FASE 4).

ML is a *probabilistic filter* only: it estimates P(up)/P(down)/P(neutral)
from the ten timestamped feature categories of a FeatureSnapshot and feeds
the `ml` dimension of the Signal Score. It never controls execution; the
Risk Engine (FASE 5) keeps final authority and can reject anything.

No look-ahead: features are always frozen at decision time `t`, and the
walk-forward split keeps a gap >= horizon so a training label never overlaps
test features.
"""

from crypto_scalper.ml.confidence import ConfidenceProperties, confidence_from_proba
from crypto_scalper.ml.features import (
    CLASS_NAMES,
    LabeledDataset,
    ValueSchema,
    build_dataset,
    forward_return_labels,
)
from crypto_scalper.ml.predictor import MLPrediction, MLPredictor

__all__ = [
    "CLASS_NAMES",
    "ConfidenceProperties",
    "LabeledDataset",
    "MLPrediction",
    "MLPredictor",
    "ValueSchema",
    "build_dataset",
    "confidence_from_proba",
    "forward_return_labels",
]