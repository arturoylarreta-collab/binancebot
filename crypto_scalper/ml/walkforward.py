"""Anchored walk-forward splitting and evaluation metrics (FASE 4).

WalkForwardSplit produces *chronological*, non-overlapping test folds with a
strict gap >= horizon between the latest training label end and the earliest
test observation start. This eliminates look-ahead bias completely.

Split description (n_windows = 4 → 3 usable folds):
  edges = linspace(t_min, t_max, n_windows+1)
  fold k  (k=0..n_windows-2):
      test  = {ts in [edges[k+1], edges[k+2])}
      train = {ts + horizon_ms <= edges[k+1]}

The first window [edges[0], edges[1]) serves as the seed for fold 0's test
and is never itself a test fold, so the model always sees the historical
tail of that seed. In the final holdout everything before a fraction of the
series is used for training with the same gap.

No shuffling ever. Every sample appears in exactly one test fold.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
from sklearn.metrics import accuracy_score, log_loss


# ── metrics ────────────────────────────────────────────────────────────────────


def multiclass_brier(y_true: np.ndarray, proba: np.ndarray) -> float:
    """Mean multiclass Brier score over all samples."""
    y = np.asarray(y_true, dtype=np.int32)
    p = np.asarray(proba, dtype=np.float64)
    n = len(y)
    if n == 0:
        return 0.0
    indicator = (np.arange(p.shape[1])[None, :] == y[:, None]).astype(np.float64)
    return float(np.mean(np.sum((p - indicator) ** 2, axis=1)))


def expected_calibration_error(
    y_true: np.ndarray,
    proba: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Classification ECE: mean |acc(bin) - conf(bin)| weighted by bin size."""
    y = np.asarray(y_true, dtype=np.int32)
    p = np.asarray(proba, dtype=np.float64)
    n = len(y)
    if n == 0:
        return 0.0
    conf = np.max(p, axis=1)
    preds = np.argmax(p, axis=1)
    correct = (preds == y).astype(np.float64)
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    total = 0.0
    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        in_bin = (conf >= lo) & (conf < hi) if hi < 1.0 else (conf >= lo) & (conf <= hi)
        k = int(np.sum(in_bin))
        if k == 0:
            continue
        total += abs(float(np.mean(correct[in_bin])) - float(np.mean(conf[in_bin]))) * (k / n)
    return float(total)


def evaluate(
    y_true: np.ndarray,
    proba: np.ndarray,
    class_names: Tuple[str, ...] = ("down", "neutral", "up"),
) -> Dict[str, float]:
    """Full evaluation dict for one fold / holdout split."""
    yt = np.asarray(y_true, dtype=np.int32)
    p = np.asarray(proba, dtype=np.float64)
    ll = float(log_loss(yt, p, labels=list(range(len(class_names)))))
    acc = float(accuracy_score(yt, np.argmax(p, axis=1)))
    brier = multiclass_brier(yt, p)
    ece = expected_calibration_error(yt, p)
    return {"log_loss": ll, "accuracy": acc, "brier": brier, "ece": ece}


# ── fold data ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Fold:
    index: int
    train_idx: np.ndarray
    test_idx: np.ndarray
    train_start_ms: int
    train_end_ms: int
    test_start_ms: int
    test_end_ms: int


class WalkForwardSplit:
    """Chronological anchored split with a strict horizon gap.

    Parameters
    ----------
    ts: sorted timestamps (ms) of every sample (may be interleaved across
        symbols; sorted internally).
    horizon_ms: forward horizon in ms (gap = horizon_ms between latest
        training label end and test window start).
    n_windows: total windows including the seed window.
        Usable folds = n_windows - 1.
    min_train: minimum training samples per fold; raises if unsatisfied.
    min_test: minimum test samples per fold.
    """

    def __init__(
        self,
        ts: np.ndarray,
        horizon_ms: int,
        n_windows: int = 4,
        min_train: int = 20,
        min_test: int = 10,
    ) -> None:
        self._ts = np.asarray(ts, dtype=np.int64)
        self._horizon_ms = int(horizon_ms)
        self._n_windows = int(n_windows)
        self._min_train = int(min_train)
        self._min_test = int(min_test)
        if self._n_windows < 2:
            raise ValueError("n_windows must be >= 2")

    @property
    def n_folds(self) -> int:
        return self._n_windows - 1

    def _sorted_order(self) -> np.ndarray:
        return np.argsort(self._ts, kind="stable")

    def folds(self) -> Iterator[Fold]:
        order = self._sorted_order()
        ts_sorted = self._ts[order]
        t_min = int(ts_sorted[0])
        t_max = int(ts_sorted[-1])
        if t_max <= t_min:
            raise ValueError("insufficient time range for any fold")
        edges = np.linspace(t_min, t_max, self._n_windows + 1)
        H = self._horizon_ms
        for k in range(self._n_windows - 1):
            train_lo = float(edges[0])
            train_hi = float(edges[k + 1]) - H
            test_lo = float(edges[k + 1])
            test_hi = float(edges[k + 2])
            train_mask = ts_sorted <= train_hi
            test_mask = (ts_sorted >= test_lo) & (ts_sorted < test_hi) if k < self._n_windows - 2 else (ts_sorted >= test_lo) & (ts_sorted <= test_hi)
            train_idx = order[train_mask]
            test_idx = order[test_mask]
            if len(train_idx) < self._min_train:
                raise ValueError(
                    f"fold {k}: train too small ({len(train_idx)} < {self._min_train})"
                )
            if len(test_idx) < self._min_test:
                raise ValueError(
                    f"fold {k}: test too small ({len(test_idx)} < {self._min_test})"
                )
            yield Fold(
                index=k,
                train_idx=train_idx,
                test_idx=test_idx,
                train_start_ms=int(ts_sorted[train_idx.min()]) if len(train_idx) else 0,
                train_end_ms=int(ts_sorted[train_idx.max()]) if len(train_idx) else 0,
                test_start_ms=int(ts_sorted[test_idx.min()]) if len(test_idx) else 0,
                test_end_ms=int(ts_sorted[test_idx.max()]) if len(test_idx) else 0,
            )

    def calibration_split(
        self, train_idx: np.ndarray, cal_fraction: float = 0.30
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Split training data into fit (first) and calibration (last) by time."""
        if len(train_idx) == 0:
            return train_idx, train_idx
        order_within = np.argsort(self._ts[train_idx], kind="stable")
        ordered = train_idx[order_within]
        cut = max(1, int(len(ordered) * (1 - cal_fraction)))
        return ordered[:cut], ordered[cut:]

    def final_holdout_split(
        self, holdout_fraction: float = 0.20
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Split into all-but-holdout (train, with gap) and holdout (test)."""
        order = self._sorted_order()
        ts_sorted = self._ts[order]
        holdout_start = ts_sorted[-1] - int(holdout_fraction * (ts_sorted[-1] - ts_sorted[0]))
        train_mask = ts_sorted + self._horizon_ms <= holdout_start
        test_mask = ts_sorted >= holdout_start
        train_idx = order[train_mask]
        test_idx = order[test_mask]
        if len(train_idx) < self._min_train or len(test_idx) < self._min_test:
            raise ValueError(
                f"final_holdout: insufficient data train={len(train_idx)} test={len(test_idx)}"
            )
        return train_idx, test_idx

    def is_strict(self) -> bool:
        """Verify that every fold satisfies the gap constraint."""
        for fold in self.folds():
            if len(fold.test_idx) == 0 or len(fold.train_idx) == 0:
                continue
            latest_train_ts = int(self._ts[fold.train_idx].max())
            earliest_test_ts = int(self._ts[fold.test_idx].min())
            if latest_train_ts + self._horizon_ms > earliest_test_ts:
                return False
        return True