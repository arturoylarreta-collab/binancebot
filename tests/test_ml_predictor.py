"""FASE 4 — MLPredictor artifact: inference and serialization.

Covers: MLPrediction fields, probability sum, category() output keys, and
joblib round-trip (save/load) via tmp_path.
"""

from __future__ import annotations

import numpy as np
import pytest

from crypto_scalper.ml.classifier import fit_raw
from crypto_scalper.ml.features import ValueSchema
from crypto_scalper.ml.predictor import MLPrediction, MLPredictor

COLS = ("volume.volume_zscore", "momentum.rsi", "trend.adx")


def _tiny_predictor(schema: ValueSchema) -> MLPredictor:
    rng = np.random.default_rng(11)
    X = rng.normal(size=(300, len(COLS)))
    y = rng.integers(0, 3, 300)
    X[:, 0] += 0.9 * (y - 1)
    model = fit_raw("logistic", X, y, seed=1)
    return MLPredictor(
        model=model,
        schema=schema,
        horizon_s=30,
        threshold=0.0015,
        model_version="logistic-v1",
        model_name="logistic",
    )


def _bundle() -> dict:
    return {
        "volume": {"volume_zscore": 2.0, "relative_volume": 1.2},
        "momentum": {"rsi": 62.0},
        "trend": {"adx": 28.0, "trend_alignment": 1.0},
    }


class TestMLPredictor:
    def test_predict_fields(self):
        pred = _tiny_predictor(ValueSchema(columns=COLS)).predict(_bundle())
        assert isinstance(pred, MLPrediction)
        assert pred.p_up + pred.p_down + pred.p_neutral == pytest.approx(1.0)
        assert 0.0 <= pred.ml_score <= 100.0
        assert 0.0 <= pred.uncertainty <= 1.0
        assert pred.model_version == "logistic-v1"
        assert pred.dominant_class_name in ("down", "neutral", "up")

    def test_category_keys(self):
        p = _tiny_predictor(ValueSchema(columns=COLS))
        cat = p.category(_bundle())
        assert set(cat.keys()) == {
            "p_up", "p_down", "p_neutral", "ml_score",
            "confidence", "uncertainty", "edge",
        }
        # ml_score is consistent with predict()
        pred = p.predict(_bundle())
        assert cat["ml_score"] == pytest.approx(pred.ml_score)

    def test_missing_category_leaves_zero_filled(self):
        p = _tiny_predictor(ValueSchema(columns=COLS))
        # schema is fixed to the trained column count; a bundle that is missing
        # a whole category still yields a valid row with zeros in those slots.
        pred = p.predict({"volume": {"volume_zscore": 2.0}})
        from crypto_scalper.ml.features import ValueSchema as VS

        row = VS(columns=COLS).to_row({"volume": {"volume_zscore": 2.0}})
        assert row[p.schema._column_index()["trend.adx"]] == 0.0
        assert 0.0 <= pred.ml_score <= 100.0

    def test_roundtrip_joblib(self, tmp_path):
        p = _tiny_predictor(ValueSchema(columns=COLS))
        path = tmp_path / "p.joblib"
        p.save(path)
        loaded = MLPredictor.load(path)
        assert loaded.model_version == p.model_version
        assert loaded.schema.columns == p.schema.columns
        a = p.predict(_bundle())
        b = loaded.predict(_bundle())
        assert a.ml_score == pytest.approx(b.ml_score)
        assert a.p_up == pytest.approx(b.p_up)

    def test_load_rejects_foreign_objects(self, tmp_path):
        import joblib

        path = tmp_path / "bad.joblib"
        joblib.dump({"not": "a predictor"}, str(path))
        with pytest.raises(TypeError):
            MLPredictor.load(path)