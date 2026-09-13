"""FASE 4 — ModelManager: candidate evaluation, best-model selection,
holdout honesty, and reproducibility.

Covers: walk-forward comparison of models, a final temporal holdout that is
never seen during selection, artifact shape, and deterministic re-runs.
"""

from __future__ import annotations

import numpy as np
import pytest

from crypto_scalper.ml.features import build_dataset
from crypto_scalper.ml.model_manager import ModelManager
from crypto_scalper.ml.synthetic import SyntheticMarket


def _dataset(seed=4, symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"), seconds=160):
    snaps = SyntheticMarket(symbols=symbols, seconds_per_symbol=seconds, seed=seed).generate()
    return build_dataset(snaps, horizon_s=30, threshold=0.0015)


class TestSelection:
    def test_selects_best_by_brier_on_walkforward(self):
        ds = _dataset()
        predictor, report = ModelManager(seed=4, n_windows=3).fit_best(
            ds, models=("random_forest",)
        )
        assert report.model_name == "random_forest"
        assert len(report.comparisons) == 1
        comp = report.comparisons[0]
        assert len(comp.per_fold) == 2  # 3 windows -> 2 usable folds
        assert comp.mean_brier == pytest.approx(
            np.mean([f.brier for f in comp.per_fold])
        )
        assert len(comp.top_features) == 20

    def test_holdout_metrics_are_on_never_seen_time(self):
        ds = _dataset()
        predictor, report = ModelManager(seed=4, n_windows=3).fit_best(
            ds, models=("logistic",)
        )
        oos = report.oos_metrics
        assert report.oos_n_test >= 4
        assert 0.0 <= oos["accuracy"] <= 1.0
        assert oos["brier"] <= 1.0
        # warmup gap zone is skipped, so train + holdout is a strict subset
        assert report.oos_n_train + report.oos_n_test <= ds.n_samples
        assert report.oos_n_train >= report.oos_n_test

    def test_artifact_carries_model_and_schema(self):
        ds = _dataset()
        predictor, report = ModelManager(seed=4, n_windows=3).fit_best(
            ds, models=("random_forest",)
        )
        assert predictor.model_version.endswith("-v1")
        assert predictor.model_name == "random_forest"
        assert predictor.schema.columns == tuple(ds.columns)
        assert predictor.horizon_s == 30
        # predicting on a training sample must yield a valid bounded score
        p = predictor.predict({})
        # default (all-missing) row -> neutral-ish score; must be bounded
        assert 0.0 <= p.ml_score <= 100.0
        assert np.allclose(p.p_up + p.p_down + p.p_neutral, 1.0)

    def test_seed_recovers_same_report(self):
        ds = _dataset(seed=7)
        a, rep_a = ModelManager(seed=7, n_windows=3).fit_best(
            ds, models=("logistic",))
        b, rep_b = ModelManager(seed=7, n_windows=3).fit_best(
            ds, models=("logistic",))
        assert rep_a.model_name == rep_b.model_name
        assert rep_a.oos_metrics["brier"] == pytest.approx(
            rep_b.oos_metrics["brier"], abs=1e-12
        )
        assert rep_a.comparisons[0].mean_brier == pytest.approx(
            rep_b.comparisons[0].mean_brier, abs=1e-12
        )
        assert a.model_version == b.model_version

    def test_model_comparison_ranks_by_brier(self):
        ds = _dataset(seed=5)
        predictor, report = ModelManager(seed=5, n_windows=3).fit_best(ds)
        names = [c.model_name for c in report.comparisons]
        assert set(names) == {"logistic", "random_forest", "gradient_boosting"}
        briers = [c.mean_brier for c in report.comparisons]
        assert briers == sorted(briers)
        assert report.model_name == report.comparisons[0].model_name