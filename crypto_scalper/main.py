"""crypto_scalper entry points.

Modes:

  pipeline   - full real-time market-data pipeline (producer → queue →
               processor → state → features → sink). FASE 2: no orders.
  one-shot   - fetch one snapshot of features for a symbol and print JSON
               (offline-friendly smoke check of the whole data path).

FASE 2 never generates or places orders and never reads API secrets.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import signal
import sys
from pathlib import Path
from typing import List, Optional

from crypto_scalper.config.settings import Settings
from crypto_scalper.config.symbols import SymbolRules
from crypto_scalper.core.events import EventBus
from crypto_scalper.core.exceptions import (
    ExchangeConnectionError,
    MarketDataNotReady,
    UniverseEmptyError,
)
from crypto_scalper.core.models import AggTrade, FeatureSnapshot
from crypto_scalper.external_data.store import NewsStore
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.processor import SymbolProcessor
from crypto_scalper.market_data.rest import BinanceFuturesRest
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.market_data.universe import UniverseSelector
from crypto_scalper.market_data.websocket import WebSocketManager
from crypto_scalper.monitoring.logger import setup_logging
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.storage.base import NoopRepository, Repository
from crypto_scalper.strategies.signal_engine import SignalEngine

log = logging.getLogger(__name__)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="crypto_scalper FASE 2/3 pipeline")
    p.add_argument("--env", default=None, help="dev | paper | live")
    p.add_argument(
        "--mode",
        default="pipeline",
        choices=["pipeline", "one-shot", "signal"],
        help="one-shot prints a FeatureSnapshot; signal adds the Signal breakdown",
    )
    p.add_argument("--symbols", default="", help="comma-separated override")
    p.add_argument("--log-dir", default="", help="override log directory")
    return p.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    if args.env:
        import os
        os.environ["APP_ENV"] = args.env  # Settings.load() applies the matching .env file

    settings = Settings.load()
    if args.symbols:
        settings = _override_symbols(settings, args.symbols)
    log_dir = Path(args.log_dir) if args.log_dir else settings.log_dir
    setup_logging(level=settings.log_level, log_dir=log_dir)
    log.info("starting", extra={"env": settings.environment.name, "mode": settings.run_mode})

    if settings.run_mode in ("backtest", "paper", "live"):
        log.error("unsupported run mode for FASE 2", extra={"run_mode": settings.run_mode})
        return 2

    if args.mode == "one-shot":
        return await run_one_shot(settings)

    if args.mode == "signal":
        return await run_signal_demo(settings)

    return await run_pipeline(settings)


def _override_symbols(settings: Settings, raw: str) -> Settings:
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    import dataclasses
    return dataclasses.replace(settings, explicit_symbols=symbols)


# ── Full pipeline ───────────────────────────────────────────────────────────────


async def run_pipeline(settings: Settings) -> int:
    rest = BinanceFuturesRest(settings.rest_url)
    bus = EventBus(queue_maxsize=settings.ws.event_queue_maxsize // 10)
    news_store = NewsStore()
    repo: Repository = NoopRepository()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    states: dict = {}
    processors: List[SymbolProcessor] = []
    workers: List[asyncio.Task] = []
    sink: Optional[asyncio.Task] = None
    metrics_task: Optional[asyncio.Task] = None
    ws_task: Optional[asyncio.Task] = None

    try:
        await rest.start()
        symbols = await _resolve_universe(settings, rest)

        states = {s: SymbolState(s) for s in symbols}
        processors = [
            SymbolProcessor(
                symbol=s,
                state=states[s],
                rest=rest,
                bus=bus,
                metrics=METRICS,
                depth_snapshot_limit=settings.ws.depth_snapshot_limit,
            )
            for s in symbols
        ]
        for proc in processors:
            await proc.start()

        engine = FeatureEngine(settings.features, news_store, METRICS)
        feature_out: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        signal_engine = SignalEngine(settings.strategies)

        workers = [
            asyncio.create_task(
                _feature_worker(states[s], engine, feature_out, stop_event, settings.features.interval_s)
            )
            for s in symbols
        ]
        sink = asyncio.create_task(
            _feature_sink(feature_out, repo, signal_engine, stop_event)
        )
        metrics_task = asyncio.create_task(_metrics_summary(stop_event))
        ws = WebSocketManager(settings.ws, bus, METRICS)
        ws_task = asyncio.create_task(ws.run(symbols, stop_event))

        log.info("pipeline ready", extra={"symbols": len(symbols)})
        await stop_event.wait()
    finally:
        stop_event.set()
        for t in [*workers, sink, metrics_task, ws_task]:
            if t is not None:
                t.cancel()
        await asyncio.gather(
            *[t for t in [*workers, sink, metrics_task, ws_task] if t is not None],
            return_exceptions=True,
        )
        for proc in processors:
            await proc.close()
        await repo.close()
        await rest.close()
    return 0


async def _feature_worker(
    state: SymbolState,
    engine: FeatureEngine,
    out_q: asyncio.Queue,
    stop_event: asyncio.Event,
    interval_s: float,
) -> None:
        try:
            if state.is_ready():
                snapshot = engine.compute(state)
                try:
                    out_q.put_nowait(snapshot)
                except asyncio.QueueFull:
                    METRICS.incr("features.queue_full")
                    log.error("feature queue full", extra={"symbol": state.symbol})
        except MarketDataNotReady:
            pass  # still warming up; normal before the first candles exist
        except Exception:  # noqa: BLE001
            log.exception("feature worker error", extra={"symbol": state.symbol})
        await asyncio.sleep(interval)


async def _feature_sink(
    in_q: asyncio.Queue,
    repo: Repository,
    signal_engine: SignalEngine,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            snapshot: FeatureSnapshot = await asyncio.wait_for(in_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        METRICS.incr("features.snapshots")
        METRICS.record("feature.to_decision_us", _latency_us(snapshot.timestamp_ms))
        await repo.save_feature_snapshot(snapshot)
        METRICS.incr("signals.evaluated")
        signal = signal_engine.evaluate(snapshot)
        if signal.eligible:
            METRICS.incr("signals.eligible")
        else:
            METRICS.incr("signals.gated")
        _log_signal(signal, snapshot)


def _log_signal(signal, snapshot: FeatureSnapshot) -> None:
    comp = signal.components
    log.info(
        "signal generated",
        extra={
            "symbol": signal.symbol,
            "side": signal.signal_type.name,
            "score": round(signal.score, 3),
            "regime": signal.regime,
            "eligible": signal.eligible,
            "reason": signal.reason,
            "trend": round(comp["trend"].score, 2) if "trend" in comp else None,
            "momentum": round(comp["momentum"].score, 2) if "momentum" in comp else None,
            "volume": round(comp["volume"].score, 2) if "volume" in comp else None,
            "order_book": round(comp["order_book"].score, 2) if "order_book" in comp else None,
            "volatility": round(comp["volatility"].score, 2) if "volatility" in comp else None,
            "price_structure": round(comp["price_structure"].score, 2) if "price_structure" in comp else None,
            "news": round(comp["news"].score, 2) if "news" in comp else None,
            "ml": round(comp["ml"].score, 2) if "ml" in comp else None,
            "rsi": round(snapshot.rsi, 2),
            "obi": round(snapshot.order_book_imbalance, 4),
            "volume_zscore": round(snapshot.volume_zscore, 2),
            "spread_pct": round(snapshot.spread_pct, 5),
            "news_sentiment": round(snapshot.news_sentiment, 3),
        },
    )


async def _metrics_summary(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(30.0)
        snap = METRICS.snapshot()
        log.info("metrics summary", extra=snap)


# ── One-shot verification ──────────────────────────────────────────────────────


async def run_one_shot(settings: Settings) -> int:
    rest = BinanceFuturesRest(settings.rest_url)
    news_store = NewsStore()
    try:
        await rest.start()
        symbols = await _resolve_universe(settings, rest)
        if not symbols:
            raise UniverseEmptyError("no symbols selected")
        state = await _build_state_from_rest(rest, symbols[0])
        engine = FeatureEngine(settings.features, news_store, METRICS)
        snapshot = engine.compute(state)
        print(json.dumps(snapshot.to_dict(), indent=2))
        return 0
    except (ExchangeConnectionError, MarketDataNotReady, UniverseEmptyError) as exc:
        log.error("one-shot failed", extra={"error": str(exc)})
        return 1
    finally:
        await rest.close()


# ── Signal demo (FASE 3 verification) ─────────────────────────────────────────


async def run_signal_demo(settings: Settings) -> int:
    rest = BinanceFuturesRest(settings.rest_url)
    news_store = NewsStore()
    try:
        await rest.start()
        symbols = await _resolve_universe(settings, rest)
        if not symbols:
            raise UniverseEmptyError("no symbols selected")
        state = await _build_state_from_rest(rest, symbols[0])
        engine = FeatureEngine(settings.features, news_store, METRICS)
        snapshot = engine.compute(state)
        demo_strategy = dataclasses.replace(settings.strategies, enabled=True)
        signal = SignalEngine(demo_strategy).evaluate(snapshot)
        print(json.dumps({"snapshot": snapshot.to_dict(), "signal": signal.to_dict()}, indent=2))
        return 0
    except (ExchangeConnectionError, MarketDataNotReady, UniverseEmptyError) as exc:
        log.error("signal demo failed", extra={"error": str(exc)})
        return 1
    finally:
        await rest.close()


async def _build_state_from_rest(rest: BinanceFuturesRest, symbol: str) -> SymbolState:
    state = SymbolState(symbol)
    depth = await rest.depth(symbol, limit=100)
    assert state.orderbook.apply_snapshot(
        last_update_id=int(depth["lastUpdateId"]),
        bids=depth["bids"],
        asks=depth["asks"],
        ts_ms=rest.utc_now_ms(),
    ), "snapshot replay failed"

    aggs = await rest.agg_trades(symbol, limit=500)
    for a in aggs:
        trade = AggTrade(
            symbol=symbol,
            event_time_ms=int(a["T"]),
            trade_id=int(a["a"]),
            price=float(a["p"]),
            quantity=float(a["q"]),
        )
        state.on_trade(trade)
    return state


async def _resolve_universe(settings: Settings, rest: BinanceFuturesRest) -> List[str]:
    if settings.explicit_symbols:
        return settings.explicit_symbols
    selector = UniverseSelector(settings.universe, SymbolRules(), rest, METRICS)
    selected = await selector.select()
    if not selected:
        raise UniverseEmptyError("universe selector produced no symbols")
    return [u.symbol for u in selected]


def _latency_us(snapshot_ts_ms: int) -> float:
    import time
    return max(0.0, (time.time() * 1000 - snapshot_ts_ms) * 1000)


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, ValueError, RuntimeError):
            pass


def main() -> None:
    args = parse_args()
    try:
        rc = asyncio.run(run(args))
    except KeyboardInterrupt:
        rc = 0
    sys.exit(rc)


if __name__ == "__main__":
    main()