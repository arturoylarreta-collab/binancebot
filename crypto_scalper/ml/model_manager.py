"""Model comparison and selection via walk-forward evaluation (FASE 4).

ModelManager orchestrates:
1. Evaluate every candidate model through anchored walk-forward folds with
   calibrated (isotonic) probabilities.
2. Select the best by mean Brier score across folds (lower = better).
3. Train a final calibrated estimator on all-but-holdout data and measure
   OOS metrics.
4. Return an MLPredictor artifact + a transparent SelectionReport.

Calibration stays inside each training fold (no test touch). No pandas. The
walk-forward split guarantees ts_train + horizon <= ts_test for every fold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from crypto_scalper.ml.classifier import (
    DEFAULT_MODELS,
    feature_importance,
    fit_calibrated,
    fit_raw,
    predict_proba,
)
from crypto_scalper.ml.features import LabeledDataset, ValueSchema, CLASS_NAMES
from crypto_scalper.ml.predictor import MLPredictor
from crypto_scalper.ml.walkforward import (
    WalkForwardSplit,
    evaluate,
    multiclass_brier,
)


@dataclass(frozen=True)
class FoldMetrics:
    fold: int
    log_loss: float
    accuracy: float
    brier: float
    ece: float
    n_train: int
    n_test: int
    test_start_ms: int
    test_end_ms: int


@dataclass(frozen=True)
class ModelComparison:
    model_name: str
    mean_log_loss: float
    mean_brier: float
    mean_accuracy: float
    mean_ece: float
    per_fold: Tuple[FoldMetrics, ...]
    top_features: Tuple[Tuple[str, float], ...]


@dataclass(frozen=True)
class SelectionReport:
    model_name: str
    model_version: str
    horizon_s: int
    threshold: float
    n_samples: int
    n_features: int
    comparisons: Tuple[ModelComparison, ...]
    oos_metrics: Dict[str, float]
    oos_n_train: int
    oos_n_test: int


class ModelManager:
    """Compare models via walk-forward, select best, train final predictor.

    Parameters
    ----------
    seed : int
        Control for reproducible fits.
    n_windows : int
        Total time windows (usable folds = n_windows - 1; default 4 → 3).
    holdout_fraction : float
        Fraction of the series held out for the final OOS evaluation of the
        selected model.
    """

    def __init__(
        self,
        seed: int = 0,
        n_windows: int = 4,
        holdout_fraction: float = 0.20,
    ) -> None:
        self._seed = int(seed)
        self._n_windows = int(n_windows)
        self._holdout_fraction = float(holdout_fraction)

    def evaluate_candidates(
        self,
        dataset: LabeledDataset,
        models: Optional[Tuple[str, ...]] = None,
    ) -> Dict[str, ModelComparison]:
        models = tuple(models or DEFAULT_MODELS)
        horizon_ms = int(dataset.horizon_s * 1000)
        splitter = WalkForwardSplit(
            dataset.ts,
            horizon_ms=horizon_ms,
            n_windows=self._n_windows,
            min_train=8,
            min_test=4,
        )
        results: Dict[str, ModelComparison] = {}
        for model_name in models:
            folds_data: List[FoldMetrics] = []
            last_importances: List[Tuple[str, float]] = []
            for fold in splitter.folds():
                X_tr, y_tr = dataset.X[fold.train_idx], dataset.y[fold.train_idx]
                X_te, y_te = dataset.X[fold.test_idx], dataset.y[fold.test_idx]

                calibrated = fit_calibrated(model_name, X_tr, y_tr, self._seed)
                proba = predict_proba(calibrated, X_te)
                metrics = evaluate(y_te, proba, dataset.class_names)
                folds_data.append(
                    FoldMetrics(
                        fold=fold.index,
                        log_loss=metrics["log_loss"],
                        accuracy=metrics["accuracy"],
                        brier=metrics["brier"],
                        ece=metrics["ece"],
                        n_train=len(fold.train_idx),
                        n_test=len(fold.test_idx),
                        test_start_ms=int(fold.test_start_ms),
                        test_end_ms=int(fold.test_end_ms),
                    )
                )
                if len(fold.train_idx) > 0:
                    raw = fit_raw(model_name, dataset.X[fold.train_idx], dataset.y[fold.train_idx], self._seed)
                    last_importances = feature_importance(
                        model_name, raw, tuple(dataset.columns)
                    )

            if not folds_data:
                raise ValueError(f"no valid folds for {model_name}")
            results[model_name] = ModelComparison(
                model_name=model_name,
                mean_log_loss=float(np.mean([f.log_loss for f in folds_data])),
                mean_brier=float(np.mean([f.brier for f in folds_data])),
                mean_accuracy=float(np.mean([f.accuracy for f in folds_data])),
                mean_ece=float(np.mean([f.ece for f in folds_data])),
                per_fold=tuple(folds_data),
                top_features=tuple(last_importances),
            )
        return results

    def fit_best(
        self,
        dataset: LabeledDataset,
        models: Optional[Tuple[str, ...]] = None,
    ) -> Tuple[MLPredictor, SelectionReport]:
        comparisons_dict = self.evaluate_candidates(dataset, models)
        comparisons = tuple(
            sorted(comparisons_dict.values(), key=lambda c: c.mean_brier)
        )
        best = comparisons[0]
        horizon_ms = int(dataset.horizon_s * 1000)
        splitter = WalkForwardSplit(
            dataset.ts,
            horizon_ms=horizon_ms,
            n_windows=self._n_windows,
            min_train=8,
            min_test=4,
        )
        train_idx, hold_idx = splitter.final_holdout_split(self._holdout_fraction)
        X_tr, y_tr = dataset.X[train_idx], dataset.y[train_idx]
        X_hold, y_hold = dataset.X[hold_idx], dataset.y[hold_idx]

        final_model = fit_calibrated(best.model_name, X_tr, y_tr, self._seed)
        hold_proba = predict_proba(final_model, X_hold)
        oos = evaluate(y_hold, hold_proba, dataset.class_names)

        version = f"{best.model_name}-v1"
        predictor = MLPredictor(
            model=final_model,
            schema=ValueSchema(columns=dataset.columns),
            horizon_s=dataset.horizon_s,
            threshold=dataset.threshold,
            model_version=version,
            model_name=best.model_name,
        )
        report = SelectionReport(
            model_name=best.model_name,
            model_version=version,
            horizon_s=dataset.horizon_s,
            threshold=dataset.threshold,
            n_samples=dataset.n_samples,
            n_features=dataset.n_features,
            comparisons=comparisons,
            oos_metrics=oos,
            oos_n_train=len(train_idx),
            oos_n_test=len(hold_idx),
        )
        return predictor, report