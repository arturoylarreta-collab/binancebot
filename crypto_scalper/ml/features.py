"""Feature vectorization and forward-return labeling (FASE 4).

ValueSchema flattens the ten feature categories into a deterministic numeric
row. Only scalar values that were observable at decision time are used
(strings such as "regime_name" are dropped, missing keys fill with 0.0);
the feature *never* contains future information.

Labels use FUTURE price only as the supervised target — that is their
definition (P(ret_{t->t+H} >= threshold)). The feature vector omits it; the
walk-forward split (ml/walkforward.py) enforces ts + horizon <= train cut so
no training label can overlap a test observation.

No pandas here; the decision path is pure numpy (hot path constraint).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np

from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.features import categories

CLASS_NAMES: Tuple[str, ...] = ("down", "neutral", "up")
_DOWN, _NEUTRAL, _UP = 0, 1, 2

_SKIPPED_VALUE_TYPES = (str, dict, list, tuple)
_SKIPPED_KEYS = ("regime_name",)


@dataclass(frozen=True)
class ValueSchema:
    """Deterministic column order built from a corpus of category dicts."""

    columns: Tuple[str, ...]

    @property
    def num_features(self) -> int:
        return len(self.columns)

    @classmethod
    def from_categories(
        cls, features_bundle: Iterable[Dict[str, Dict[str, float]]]
    ) -> "ValueSchema":
        keys: set = set()
        for bundle in features_bundle:
            for cat_name in categories.CATEGORY_KEYS:
                bucket = bundle.get(cat_name)
                if not isinstance(bucket, dict):
                    continue
                for key, value in bucket.items():
                    if key in _SKIPPED_KEYS or value is None:
                        continue
                    if isinstance(value, _SKIPPED_VALUE_TYPES):
                        continue
                    keys.add(f"{cat_name}.{key}")
        return cls(columns=tuple(sorted(keys)))

    def _column_index(self) -> Dict[str, int]:
        return {name: i for i, name in enumerate(self.columns)}

    def to_row(self, features_by_category: Dict[str, Dict[str, float]]) -> np.ndarray:
        idx = self._column_index()
        row = np.zeros(self.num_features, dtype=np.float64)
        for cat_name in categories.CATEGORY_KEYS:
            bucket = features_by_category.get(cat_name)
            if not isinstance(bucket, dict):
                continue
            for key, value in bucket.items():
                if key in _SKIPPED_KEYS or value is None:
                    continue
                if isinstance(value, _SKIPPED_VALUE_TYPES):
                    continue
                col = idx.get(f"{cat_name}.{key}")
                if col is not None:
                    row[col] = float(value)
        np.nan_to_num(row, copy=False, nan=0.0, posinf=0.0, neginf=0.0)
        return row

    def to_matrix(
        self, features_bundle: Sequence[Dict[str, Dict[str, float]]]
    ) -> np.ndarray:
        rows = [self.to_row(b) for b in features_bundle]
        return np.asarray(rows, dtype=np.float64)


def forward_return_labels(
    prices: np.ndarray,
    ts: np.ndarray,
    horizon_ms: int,
    threshold: float,
) -> np.ndarray:
    """O(n) labels from forward returns.

    For sample i: j = first index with ts[j] >= ts[i] + horizon_ms.
    ret = prices[j]/prices[i] - 1 -> UP / NEUTRAL / DOWN, or -1 (masked)
    when no future sample exists for that horizon. `prices`/`ts` must be
    ascending in time.
    """
    prices = np.asarray(prices, dtype=np.float64)
    ts = np.asarray(ts, dtype=np.int64)
    n = len(prices)
    labels = np.full(n, -1, dtype=np.int8)
    target = ts + int(horizon_ms)
    j = 0
    for i in range(n):
        if j <= i:
            j = i + 1
        while j < n and ts[j] < target[i]:
            j += 1
        if j >= n:
            continue
        ret = prices[j] / prices[i] - 1.0
        if ret >= threshold:
            labels[i] = _UP
        elif ret <= -threshold:
            labels[i] = _DOWN
        else:
            labels[i] = _NEUTRAL
    return labels


@dataclass
class LabeledDataset:
    """Design matrix + temporal metadata for one training corpus."""

    X: np.ndarray
    y: np.ndarray
    ts: np.ndarray
    symbols: np.ndarray
    horizon_s: int
    threshold: float
    columns: Tuple[str, ...] = ()
    class_names: Tuple[str, ...] = CLASS_NAMES

    @property
    def n_samples(self) -> int:
        return int(len(self.y))

    @property
    def n_features(self) -> int:
        return int(self.X.shape[1])

    def class_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in self.y:
            counts[self.class_names[int(row)]] = counts.get(self.class_names[int(row)], 0) + 1
        return counts


def build_dataset(
    snapshots: Sequence[FeatureSnapshot],
    horizon_s: int = 30,
    threshold: float = 0.0015,
) -> LabeledDataset:
    """Assemble a LabeledDataset from timestamped snapshots (offline).

    Labels are computed per symbol from that symbol's future price series
    only; features are vectorized with a schema learned from the whole corpus
    (deterministic). Masked samples (no future within horizon) are dropped.
    """
    if not snapshots:
        raise ValueError("build_dataset requires at least one snapshot")

    schema = ValueSchema.from_categories(
        [s.features_by_category for s in snapshots]
    )
    by_symbol: Dict[str, List[FeatureSnapshot]] = defaultdict(list)
    for snap in snapshots:
        by_symbol[snap.symbol].append(snap)

    rows: List[np.ndarray] = []
    y_list: List[int] = []
    ts_list: List[int] = []
    sym_list: List[str] = []
    horizon_ms = int(horizon_s * 1000)

    for symbol in sorted(by_symbol):
        group = sorted(by_symbol[symbol], key=lambda s: s.timestamp_ms)
        prices = np.asarray([float(s.price) for s in group], dtype=np.float64)
        ts_arr = np.asarray([s.timestamp_ms for s in group], dtype=np.int64)
        labels = forward_return_labels(prices, ts_arr, horizon_ms, threshold)
        for snap, label in zip(group, labels):
            if label < 0:
                continue
            rows.append(schema.to_row(snap.features_by_category))
            y_list.append(int(label))
            ts_list.append(snap.timestamp_ms)
            sym_list.append(snap.symbol)

    X = np.asarray(rows, dtype=np.float64)
    y = np.asarray(y_list, dtype=np.int8)
    ts = np.asarray(ts_list, dtype=np.int64)
    symbols = np.asarray(sym_list, dtype=object)

    order = np.argsort(ts, kind="stable")
    return LabeledDataset(
        X=X[order],
        y=y[order],
        ts=ts[order],
        symbols=symbols[order],
        horizon_s=int(horizon_s),
        threshold=float(threshold),
        columns=schema.columns,
    )