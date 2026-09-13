"""Offline FASE 4 demo runner.

Usage:
    python -m crypto_scalper.ml_train [--symbols BTCUSDT,ETHUSDT] [--seconds 140]
                                      [--seed 42] [--out models/ml-predictor.joblib]

Pipeline:
    1. Generate synthetic market data offline (no network) through the real
       FeatureEngine.
    2. Build a labeled dataset (forward-return labels, feature vectorization).
    3. Compare LogisticRegression vs RandomForest vs GradientBoosting with
       anchored walk-forward, calibrated probabilities (isotonic).
    4. Select best by mean Brier score, fit final calibrated model on
       all-but-holdout data, report OOS metrics.
    5. If --out is given, persist the MLPredictor artifact (joblib) and
       round-trip it back to prove it loads.

The demo also evaluates one snapshot end-to-end through FeatureEngine +
SignalEngine to prove the `ml` dimension of the Signal Score is no longer
hard-coded to 50.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from crypto_scalper.config.strategies import StrategyConfig
from crypto_scalper.ml.features import build_dataset
from crypto_scalper.ml.model_manager import ModelManager
from crypto_scalper.ml.predictor import MLPredictor
from crypto_scalper.ml.synthetic import SyntheticMarket
from crypto_scalper.strategies.signal_engine import SignalEngine


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="crypto_scalper FASE 4 offline ML demo")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT")
    p.add_argument("--seconds", type=int, default=180)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--horizon", type=int, default=30, help="forward horizon in seconds")
    p.add_argument("--threshold", type=float, default=0.0015, help="label band (+/-)")
    p.add_argument("--out", default="", help="optional path to persist the artifact")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    if not symbols:
        print("error: --symbols cannot be empty", file=sys.stderr)
        return 2

    print(f"[1/5] generating synthetic market offline: {symbols} ({args.seconds}s)")
    snapshots = SyntheticMarket(
        symbols=symbols,
        seconds_per_symbol=args.seconds,
        seed=args.seed,
    ).generate()
    print(f"      produced {len(snapshots)} FeatureSnapshots")

    print(f"[2/5] building labeled dataset (horizon={args.horizon}s, threshold={args.threshold})")
    dataset = build_dataset(snapshots, horizon_s=args.horizon, threshold=args.threshold)
    print(f"      {dataset.n_samples} labeled samples, {dataset.n_features} features")
    print(f"      class counts: {json.dumps(dataset.class_counts())}")

    print("[3/5] walk-forward comparison of logistic / random_forest / gradient_boosting")
    manager = ModelManager(seed=args.seed)
    predictor, report = manager.fit_best(dataset)
    for comp in report.comparisons:
        print(
            f"      {comp.model_name:<18} brier={comp.mean_brier:.4f} "
            f"log_loss={comp.mean_log_loss:.4f} acc={comp.mean_accuracy:.3f} "
            f"ece={comp.mean_ece:.4f}"
        )
    print(f"      best = {report.model_name}  oos={json.dumps(report.oos_metrics, indent=2)}")

    top = report.comparisons[0].top_features[:8]
    print("[4/5] top features (selected model):")
    for name, imp in top:
        print(f"      {name:<40} {imp:.4f}")

    if args.out:
        out_path = Path(args.out)
        predictor.save(out_path)
        loaded = MLPredictor.load(out_path)
        print(f"[4.5] artifact persisted -> {out_path} and round-tripped (version {loaded.model_version})")

    print("[5/5] end-to-end check: one snapshot through FeatureEngine + SignalEngine")
    ml_snapshots = SyntheticMarket(
        symbols=symbols,
        seconds_per_symbol=args.seconds,
        seed=args.seed + 1000,
        predictor=predictor,
    ).generate()
    snap_with_ml = ml_snapshots[-1]
    ml_cat = snap_with_ml.features_by_category.get("ml", {})
    strategy = StrategyConfig(enabled=True)
    signal = SignalEngine(strategy).evaluate(snap_with_ml)
    ml_comp = signal.components.get("ml")
    print(f"      ml category  p_up={ml_cat.get('p_up'):.3f} p_down={ml_cat.get('p_down'):.3f} "
          f"ml_score={ml_cat.get('ml_score'):.1f}")
    print(f"      Signal       score={signal.score:.2f} type={signal.signal_type.name} "
          f"eligible={signal.eligible} ml_component={ml_comp.score if ml_comp else 'n/a'}")
    if ml_comp is None:
        print("error: ml component missing from signal", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())