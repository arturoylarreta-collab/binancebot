"""FASE 4 — anchored walk-forward split and evaluation metrics.

Covers: temporal correctness (gap >= horizon between train label end and
test start, no shuffle), disjoint exhaustive test coverage, monotone train
growth, calibration split chronology, final holdout, and the multiclass
scoring helpers (log-loss / Brier / ECE).
"""

from __future__ import annotations

import numpy as np
import pytest

from crypto_scalper.ml.walkforward import (
    WalkForwardSplit,
    evaluate,
    expected_calibration_error,
    multiclass_brier,
)


def _make_ts(n=1200, span_s=150, start=1_700_000_000_000):
    # 1200 timestamps over 150 s -> ~125 ms steps so early folds still
    # accumulate >= min_train samples after the anchoring gap.
    return (start + np.arange(n) * (span_s * 1000) // n).astype(np.int64)


class TestWalkForwardSplit:
    def test_folds_cover_every_sample_exactly_once(self):
        ts = _make_ts()
        split = WalkForwardSplit(ts, horizon_ms=30_000, n_windows=4)
        seen = np.zeros(len(ts), dtype=bool)
        for fold in split.folds():
            assert len(fold.test_idx) > 0
            assert not seen[fold.test_idx].any()
            seen[fold.test_idx] = True
        assert split.n_folds == 3
        # Test windows form ONE contiguous block: the warmup zone before the
        # first fold is unseen, and the tail after the last edge is unseen.
        covered = np.where(seen)[0]
        assert len(covered) > 0
        i0, i1 = int(covered[0]), int(covered[-1])
        assert seen[i0 : i1 + 1].all()     # no holes inside
        assert not seen[:i0].any()         # warmup (gap zone) is unseen
        assert not seen[i1 + 1 :].any()    # final holdout tail is unseen

    def test_gap_constraint_holds_for_every_fold(self):
        ts = _make_ts()
        H = 30_000
        split = WalkForwardSplit(ts, horizon_ms=H, n_windows=4)
        assert split.is_strict()
        for fold in split.folds():
            last_train = int(ts[fold.train_idx].max())
            first_test = int(ts[fold.test_idx].min())
            assert last_train + H <= first_test  # no label can overlap test

    def test_training_grows_monotonically(self):
        ts = _make_ts()
        split = WalkForwardSplit(ts, horizon_ms=30_000, n_windows=4)
        sizes = [len(f.train_idx) for f in split.folds()]
        assert sizes == sorted(sizes)
        assert sizes[0] < sizes[-1]

    def test_no_random_shuffle_deterministic(self):
        ts = _make_ts()
        s1 = WalkForwardSplit(ts, horizon_ms=30_000, n_windows=4)
        s2 = WalkForwardSplit(ts, horizon_ms=30_000, n_windows=4)
        for f1, f2 in zip(s1.folds(), s2.folds()):
            assert np.array_equal(f1.train_idx, f2.train_idx)
            assert np.array_equal(f1.test_idx, f2.test_idx)

    def test_windows_too_small_raises(self):
        ts = _make_ts()
        with pytest.raises(ValueError):
            WalkForwardSplit(ts, horizon_ms=120_000, n_windows=4).folds().__next__()


class TestCalibrationSplit:
    def test_fit_and_cal_are_chronological_and_disjoint(self):
        ts = _make_ts()
        split = WalkForwardSplit(ts, horizon_ms=30_000, n_windows=4)
        n = len(ts)
        fit_idx, cal_idx = split.calibration_split(np.arange(n), cal_fraction=0.30)
        assert set(fit_idx).isdisjoint(set(cal_idx))
        assert int(ts[fit_idx].max()) <= int(ts[cal_idx].min())
        assert len(fit_idx) + len(cal_idx) == n
        assert 0 < len(cal_idx) <= int(n * 0.31)

    def test_final_holdout_gap(self):
        ts = _make_ts()
        H = 30_000
        split = WalkForwardSplit(ts, horizon_ms=H, n_windows=4)
        train_idx, hold_idx = split.final_holdout_split(holdout_fraction=0.2)
        assert int(ts[train_idx].max()) + H <= int(ts[hold_idx].min())


class TestMetrics:
    def test_multiclass_brier_hand_calc(self):
        y = np.array([0, 1, 2])
        p = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
        assert multiclass_brier(y, p) == pytest.approx(0.0)

    def test_brier_bounded(self):
        rng = np.random.default_rng(0)
        y = rng.integers(0, 3, 200)
        p = rng.dirichlet([1.0, 1.0, 1.0], size=200)
        assert 0.0 <= multiclass_brier(y, p) <= 1.0

    def test_ece_in_unit_interval(self):
        rng = np.random.default_rng(1)
        y = rng.integers(0, 3, 500)
        p = rng.dirichlet([1.0, 1.0, 1.0], size=500)
        ece = expected_calibration_error(y, p)
        assert 0.0 <= ece <= 1.0

    def test_perfect_model_metrics(self):
        rng = np.random.default_rng(2)
        y = rng.integers(0, 3, 100)
        p = np.zeros((100, 3))
        p[np.arange(100), y] = 1.0
        m = evaluate(y, p)
        assert m["accuracy"] == pytest.approx(1.0)
        assert m["brier"] == pytest.approx(0.0)
        assert m["ece"] == pytest.approx(0.0)
        assert m["log_loss"] < 1e-6

    def test_average_model_is_worse_than_perfect(self):
        rng = np.random.default_rng(3)
        n = 500
        y = rng.integers(0, 3, n)
        uniform = np.full((n, 3), 1 / 3)
        perfect = np.zeros((n, 3))
        perfect[np.arange(n), y] = 1.0
        assert multiclass_brier(y, uniform) > multiclass_brier(y, perfect)
        assert evaluate(y, uniform)["brier"] > 0.5