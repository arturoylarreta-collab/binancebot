"""FASE 4 — feature vectorization and forward-return labeling.

Covers: deterministic schema + column order, missing-key/non-finite
neutralization, string exclusion, forward-return labels (up/neutral/down
with tail masking), and the assembled LabeledDataset (global time order,
finite matrix, class names).
"""

from __future__ import annotations

import numpy as np
import pytest

from crypto_scalper.ml.features import (
    LabeledDataset,
    ValueSchema,
    build_dataset,
    forward_return_labels,
)


class TestValueSchema:
    def test_columns_deterministic_and_sorted(self):
        cats1 = {"volume": {"volume_zscore": 1.0, "relative_volume": 2.0}}
        cats2 = {"momentum": {"rsi": 55.0}}
        s1 = ValueSchema.from_categories([cats1, cats2])
        s2 = ValueSchema.from_categories([cats2, cats1])
        assert s1.columns == s2.columns
        assert list(s1.columns) == sorted(s1.columns)
        assert "volume.volume_zscore" in s1.columns
        assert "momentum.rsi" in s1.columns

    def test_row_fills_present_values_and_defaults_missing(self):
        schema = ValueSchema.from_categories([
            {"volume": {"volume_zscore": 1.0}, "momentum": {"rsi": 55.0}},
        ])
        row = schema.to_row({"volume": {"volume_zscore": 0.5}})
        idx = schema._column_index()
        assert row[idx["volume.volume_zscore"]] == pytest.approx(0.5)
        assert row[idx["momentum.rsi"]] == 0.0  # missing -> 0

    def test_strings_and_nan_are_not_kept_as_features(self):
        cats = {
            "market_regime": {"regime_name": "trending_up", "regime": 0.0},
            "volume": {"volume_zscore": 1.0},
        }
        schema = ValueSchema.from_categories([cats])
        assert "market_regime.regime_name" not in schema.columns
        assert "market_regime.regime" in schema.columns

        import math
        cats_nan = {
            "volume": {"volume_zscore": float("nan"), "relative_volume": float("inf")},
        }
        schema_nan = ValueSchema.from_categories([cats_nan])
        row = schema_nan.to_row(cats_nan)
        assert np.isfinite(row).all()
        assert float(row[0]) == pytest.approx(0.0)

    def test_to_matrix_shape(self):
        schema = ValueSchema.from_categories([
            {"volume": {"volume_zscore": 1.0, "relative_volume": 2.0}},
        ])
        m = schema.to_matrix([
            {"volume": {"volume_zscore": 1.0, "relative_volume": 2.0}},
            {"volume": {"volume_zscore": 3.0}},
        ])
        assert m.shape == (2, 2)


class TestForwardReturnLabels:
    def test_up_down_neutral_detection(self):
        prices = np.array([100.0, 100.02, 100.0, 99.9, 100.1])
        ts = np.array([0, 1000, 2000, 3000, 4000])
        # horizon=2000ms, threshold=0.0005 (5bp)
        labels = forward_return_labels(prices, ts, horizon_ms=2000, threshold=0.0005)
        # i0 -> j=2, ret=0.0 -> neutral
        # i1 -> j=3, ret=-0.0012 -> down
        # i2 -> j=4, ret=+0.001 -> up
        # i3 -> j=none -> masked
        assert labels[0] == 1
        assert labels[1] == 0
        assert labels[2] == 2
        assert labels[3] == -1

    def test_tail_masked_without_future(self):
        labels = forward_return_labels(
            np.array([100.0, 100.001, 100.002]),
            np.array([0, 1000, 2000]),
            horizon_ms=10000,
            threshold=0.001,
        )
        assert (labels == -1).all()

    def test_strict_future_lookup_never_backwards(self):
        prices = np.array([100.0, 99.0, 98.0, 97.0, 96.0])
        ts = np.array([0, 1000, 2000, 3000, 4000])
        labels = forward_return_labels(prices, ts, horizon_ms=3000, threshold=0.01)
        assert labels[0] == 0  # -3% over 3s -> down
        assert labels[1] == 0
        assert labels[2] == -1  # no future


class TestBuildDataset:
    SNAPS = []  # built lazily to avoid importing heavy modules at collection

    @pytest.fixture(autouse=True)
    def _make_snaps(self):
        from crypto_scalper.features.feature_engine import FeatureEngine
        from crypto_scalper.config.settings import FeatureConfig
        from crypto_scalper.core.enums import AggressorSide
        from crypto_scalper.core.models import AggTrade
        from crypto_scalper.external_data.store import NewsStore
        from crypto_scalper.market_data.state import SymbolState
        from crypto_scalper.monitoring.metrics import Metrics

        t0 = 1_700_000_000_000
        store = NewsStore(ttl_ms=60_000)
        engine = FeatureEngine(FeatureConfig(), store, Metrics())
        out = []
        for sym, drift in (("BTCUSDT", 0.01), ("ETHUSDT", -0.01)):
            state = SymbolState(sym)
            state.orderbook.apply_snapshot(
                100, [["99", "10"]], [["101", "10"]], ts_ms=t0
            )
            price = 100.0
            for sec in range(40):
                for k in range(3):
                    is_buy = k < 2
                    price += drift
                    state.on_trade(
                        AggTrade(
                            sym, t0 + sec * 1000 + k, sec * 3 + k, price, 1.0,
                            aggressor=AggressorSide.BUY if is_buy else AggressorSide.SELL,
                        )
                    )
                if sec >= 1:
                    out.append(engine.compute(state, now_ms=t0 + sec * 1000 + 2))
        TestBuildDataset.SNAPS = out

    def test_dataset_fields(self):
        ds = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        assert isinstance(ds, LabeledDataset)
        assert ds.X.shape[0] == ds.n_samples
        assert ds.X.shape[1] == ds.n_features
        assert ds.n_features >= 5
        assert np.isfinite(ds.X).all()
        assert set(np.unique(ds.y)) <= {0, 1, 2}
        assert ds.class_names == ("down", "neutral", "up")
        assert len(ds.columns) == ds.n_features

    def test_dataset_is_sorted_by_time(self):
        ds = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        assert np.all(np.diff(ds.ts) >= 0)

    def test_dataset_never_includes_future_features(self):
        ds = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        # every row maps to exactly one snapshot's features_by_category at
        # that row's ts (and symbol): reconstructing the row from the snapshot
        # must equal X — features are frozen at decision time, nothing else.
        by_key = {}
        for s in TestBuildDataset.SNAPS:
            by_key[(s.timestamp_ms, s.symbol)] = s
        from crypto_scalper.ml.features import ValueSchema
        schema = ValueSchema(columns=ds.columns)
        for i in range(len(ds.ts)):
            snap = by_key[(int(ds.ts[i]), str(ds.symbols[i]))]
            assert np.allclose(schema.to_row(snap.features_by_category), ds.X[i])

    def test_labels_follow_declared_horizon_and_threshold(self):
        ds = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        assert ds.horizon_s == 10
        assert ds.threshold == pytest.approx(0.001)

    def test_dataset_reproducible(self):
        a = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        b = build_dataset(TestBuildDataset.SNAPS, horizon_s=10, threshold=0.001)
        assert np.array_equal(a.X, b.X)
        assert np.array_equal(a.y, b.y)
        assert a.columns == b.columns

    def test_empty_input_rejected(self):
        with pytest.raises(ValueError):
            build_dataset([])