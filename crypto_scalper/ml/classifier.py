"""Classifier factories, calibration and class-aligned probabilities (FASE 4).

Two candidate contrasts are run per model name: the *base* estimator (raw
probability) and the *calibrated* estimator. Calibration uses
`CalibratedClassifierCV(method='isotonic', cv=3)` fitted entirely inside the
training fold — no hold-out data is ever touched until evaluation.

`predict_proba` is class-aligned: the classifier's internal `classes_` are
mapped onto the fixed [down, neutral, up] = [0, 1, 2] order so a fold that
happens to lack one class still yields a (n, 3) matrix (missing classes get
0.0). This keeps the probability contract stable for the ml dimension.

No pandas. XGBoost is not installed and not justified yet: the three
candidates are compared honestly and the best is selected by Brier score.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

_CLASS_ORDER = (0, 1, 2)  # down, neutral, up  (fixed, sorted)
NUM_CLASSES = len(_CLASS_ORDER)

DEFAULT_MODELS = ("logistic", "random_forest", "gradient_boosting")


# ── base factories ─────────────────────────────────────────────────────────────


def _logistic(seed: int = 0) -> Pipeline:
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=1000,
                    tol=1e-4,
                    C=1.0,
                    class_weight="balanced",
                    solver="lbfgs",
                    random_state=seed,
                ),
            ),
        ]
    )


def _random_forest(seed: int = 0) -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=120,
        max_depth=5,
        min_samples_leaf=10,
        class_weight="balanced",
        n_jobs=-1,
        random_state=seed,
    )


def _gradient_boosting(seed: int = 0) -> GradientBoostingClassifier:
    return GradientBoostingClassifier(
        n_estimators=120,
        max_depth=3,
        min_samples_leaf=20,
        learning_rate=0.05,
        subsample=0.8,
        random_state=seed,
    )


_FACTORY: Dict[str, Any] = {
    "logistic": _logistic,
    "random_forest": _random_forest,
    "gradient_boosting": _gradient_boosting,
}


def assert_min_classes(y: np.ndarray, model_name: str, context: str) -> None:
    """Guard: an estimator needs at least two classes in the data it is fit on."""
    unique = np.unique(y)
    if len(unique) < 2:
        raise ValueError(
            f"{model_name} ({context}) requires >=2 classes, got {unique.tolist()}"
        )


# ── calibration ────────────────────────────────────────────────────────────────


def fit_calibrated(
    model_name: str,
    X: np.ndarray,
    y: np.ndarray,
    seed: int = 0,
) -> Any:
    """Fit a calibrated classifier on (X, y); calibration stays inside the
    training fold (isotonic, K-fold CV) so the test split is untouched.

    Graceful degradation: when the rarest class has too few examples for a
    hold-out to cover every class (common in tiny walk-forward folds), the
    base estimator is returned uncalibrated instead of crashing. predict_proba
    still aligns its output to the fixed 3-column (down, neutral, up) grid.
    """
    if model_name not in _FACTORY:
        raise ValueError(f"unknown model: {model_name!r}")
    assert_min_classes(y, model_name, "fit_calibrated")
    base = _FACTORY[model_name](seed=seed)

    counts = np.bincount(y.astype(int), minlength=NUM_CLASSES)
    nonzero = counts[counts > 0]
    minority = int(nonzero.min())

    # calibrating needs a per-fold validation split that still holds every
    # class; require at least 2 examples of the rarest class per fold side.
    max_cv = min(3, minority // 2)
    if max_cv < 2:
        return base.fit(X, y)

    calibrated = CalibratedClassifierCV(
        estimator=base, method="isotonic", cv=max_cv
    )
    calibrated.fit(X, y)
    return calibrated


def fit_raw(model_name: str, X: np.ndarray, y: np.ndarray, seed: int = 0) -> Any:
    """Fit the base estimator without calibration (for comparison/importances)."""
    if model_name not in _FACTORY:
        raise ValueError(f"unknown model: {model_name!r}")
    assert_min_classes(y, model_name, "fit_raw")
    base = _FACTORY[model_name](seed=seed)
    base.fit(X, y)
    return base


def predict_proba(model: Any, X: np.ndarray) -> np.ndarray:
    """(n, 3) probabilities in [down, neutral, up]; class-aligned.

    Mapping protects against folds/estimators that expose a subset of the
    three classes (missing classes get probability 0.0).
    """
    p = np.asarray(model.predict_proba(X), dtype=np.float64)
    labels = np.asarray(model.classes_, dtype=np.int64)
    out = np.zeros((p.shape[0], NUM_CLASSES), dtype=np.float64)
    for j, class_label in enumerate(labels):
        if int(class_label) in _CLASS_ORDER:
            out[:, int(class_label)] = p[:, j]
    return out


# ── feature importance ─────────────────────────────────────────────────────────


def feature_importance(
    model_name: str, model: Any, columns: Tuple[str, ...]
) -> List[Tuple[str, float]]:
    """Extract importance for the given estimator (base, non-calibrated)."""
    imps: Optional[np.ndarray] = None
    if hasattr(model, "steps") and len(model.steps) > 0:
        model = model[-1]  # unwrap Pipeline -> final estimator
    if model_name == "logistic":
        if hasattr(model, "coef_"):
            imps = np.mean(np.abs(model.coef_), axis=0)
    elif model_name in ("random_forest", "gradient_boosting"):
        if hasattr(model, "feature_importances_"):
            imps = np.asarray(model.feature_importances_, dtype=np.float64)

    if imps is None:
        imps = np.zeros(len(columns), dtype=np.float64)

    order = np.argsort(imps)[::-1]
    cols = list(columns)
    return [(cols[i], float(imps[i])) for i in order[:20]]