"""FASE 4 — classifier factories, calibration, class-aligned probabilities.

Covers: fit_calibrated API, 3-class probability contract, class alignment for
partial-class training sets, single-class input rejection, and feature
importance extraction.
"""

from __future__ import annotations

import numpy as np
import pytest

from crypto_scalper.ml.classifier import (
    DEFAULT_MODELS,
    assert_min_classes,
    feature_importance,
    fit_calibrated,
    fit_raw,
    predict_proba,
)

COLS = tuple(f"f{i}" for i in range(8))


def _data(seed=0, n=400, d=8, classes=(0, 1, 2)):
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, d))
    rng2 = np.random.default_rng(seed + 1)
    y = rng2.choice(np.array(classes), size=n)
    # add a mild signal so the model does not fit noise only
    X[:, 0] += 0.8 * (y - 1)
    X[:, 1] += 0.6 * ((y == 2).astype(float) - (y == 0).astype(float))
    return X, y


class TestFitCalibrated:
    @pytest.mark.parametrize("model_name", DEFAULT_MODELS)
    def test_probabilities_contract(self, model_name):
        X, y = _data()
        cal = fit_calibrated(model_name, X[:300], y[:300], seed=3)
        p = predict_proba(cal, X[300:])
        assert p.shape == (100, 3)
        assert np.allclose(p.sum(axis=1), 1.0, atol=1e-9)
        assert np.all((p >= 0) & (p <= 1))

    def test_class_aligned_when_training_lacks_a_class(self):
        X, y = _data(classes=(0, 2))  # no neutral class seen at fit time
        cal = fit_calibrated("logistic", X[:200], y[:200], seed=3)
        assert set(cal.classes_) == {0, 2}
        p = predict_proba(cal, X[300:])
        assert p.shape == (100, 3)
        assert np.allclose(p[:, 1], 0.0)  # neutral probability is exactly zero
        assert np.allclose(p.sum(axis=1), 1.0, atol=1e-9)

    def test_tiny_fold_degrades_gracefully(self):
        # < 4 examples of the rarest class: no calibration, raw fit still works
        rng = np.random.default_rng(9)
        X = rng.normal(size=(60, 6))
        y = np.array([0] * 29 + [2] * 29 + [1] * 2)
        cal = fit_calibrated("random_forest", X, y, seed=3)
        p = predict_proba(cal, X[:5])
        assert p.shape == (5, 3)
        assert np.allclose(p.sum(axis=1), 1.0, atol=1e-9)

    def test_single_class_rejected(self):
        X, y = np.zeros((30, 3)), np.zeros(30, dtype=int)
        with pytest.raises(ValueError):
            fit_calibrated("logistic", X, y)
        with pytest.raises(ValueError, match=">=2 classes"):
            assert_min_classes(y, "logistic", "test")

    def test_fit_raw_and_importance(self):
        X, y = _data()
        for model_name in ("logistic", "random_forest"):
            raw = fit_raw(model_name, X, y, seed=4)
            imp = feature_importance(model_name, raw, COLS)
            assert len(imp) == len(COLS)  # top-k capped at n_features
            assert all(name in COLS for name, _ in imp)
            values = [v for _, v in imp]
            assert values == sorted(values, reverse=True)
            assert values[0] > 0.0  # model used at least one feature