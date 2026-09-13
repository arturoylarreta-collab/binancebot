"""crypto_scalper entry points.

Modes:

  pipeline   - full real-time market-data pipeline (producer → queue →
               processor → state → features → sink). FASE 2: no orders.
  one-shot   - fetch one snapshot of features for a symbol and print JSON
               (offline-friendly smoke check of the whole data path).
  signal     - one-shot with the Signal Engine breakdown.
  paper      - live-pipeline with simulated execution, periodic reconciliation
               and optional one-shot trade cap (--trades N). FASE 7.

FASE 2 never generates or places orders and never reads API secrets.
FASE 7 paper mode is fully simulated; no real orders are ever sent.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from crypto_scalper.backtest.data import (
    BinanceKlinesDownloader,
    KlineCorpus,
    interval_to_param,
    klines_csv_path,
    load_klines_csv,
    save_klines_csv,
)
from crypto_scalper.backtest.engine import BacktestEngine
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
from crypto_scalper.ml.model_manager import ModelManager
from crypto_scalper.ml.predictor import MLPredictor
from crypto_scalper.monitoring.alerts import TelegramNotifier
from crypto_scalper.monitoring.logger import setup_logging
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.monitoring.observability import ObservabilityRepository
from crypto_scalper.paper.engine import PaperTradingEngine
from crypto_scalper.storage.base import NoopRepository, Repository
from crypto_scalper.storage.sqlite_repo import SqliteRepository
from crypto_scalper.strategies.signal_engine import SignalEngine

log = logging.getLogger(__name__)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="crypto_scalper pipeline / signal / paper / backtest")
    p.add_argument("--env", default=None, help="dev | paper | live")
    p.add_argument(
        "--mode",
        default="pipeline",
        choices=["pipeline", "one-shot", "signal", "paper", "backtest"],
        help="one-shot prints a FeatureSnapshot; signal adds the Signal breakdown; "
        "paper runs simulated execution with pricing/reconciliation; backtest replays "
        "historical klines through the real stack (FASE 8)",
    )
    p.add_argument("--symbols", default="", help="comma-separated override")
    p.add_argument("--log-dir", default="", help="override log directory")
    p.add_argument("--trades", type=int, default=None,
                   help="paper/backtest: stop after this many closed trades")
    p.add_argument("--start", default="",
                   help="backtest: start YYYY-MM-DD[ HH:MM:SS] (UTC) of historical window")
    p.add_argument("--end", default="",
                   help="backtest: end YYYY-MM-DD[ HH:MM:SS] (UTC) of historical window")
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

    if settings.run_mode == "live":
        log.error("live mode is not available yet", extra={"run_mode": settings.run_mode})
        return 2

    if args.mode == "one-shot":
        return await run_one_shot(settings)

    if args.mode == "signal":
        return await run_signal_demo(settings)

    if args.mode == "paper":
        return await run_paper(settings, args)

    if args.mode == "backtest":
        return await run_backtest(settings, args)

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


# ── Paper trading (FASE 7) ────────────────────────────────────────────────────


async def run_paper(settings: Settings, args: argparse.Namespace) -> int:
    """Full data pipeline + simulated execution + periodic reconciliation.

    The venue is `SimulatedExecutionAdapter`; every order stays offline and
    the audit trail goes to the SQLite repository. `--trades N` caps the
    session at N closed trades (one-shot paper).
    """
    rest = BinanceFuturesRest(settings.rest_url)
    bus = EventBus(queue_maxsize=settings.ws.event_queue_maxsize // 10)
    news_store = NewsStore()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    strategy = dataclasses.replace(settings.strategies, enabled=True)
    signal_engine = SignalEngine(strategy)
    repository, notifier = _build_observability_repository(
        SqliteRepository(settings.paper.db_path),
        settings,
        mode="paper",
        alerts=True,
    )
    feature_out: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    engine = PaperTradingEngine(
        settings=settings,
        signal_engine=signal_engine,
        source=_snapshot_source(feature_out, stop_event),
        repository=repository,
    )

    states: dict = {}
    processors: List[SymbolProcessor] = []
    workers: List[asyncio.Task] = []
    metrics_task: Optional[asyncio.Task] = None
    ws_task: Optional[asyncio.Task] = None
    engine_task: Optional[asyncio.Task] = None

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

        feature_engine = FeatureEngine(settings.features, news_store, METRICS)
        workers = [
            asyncio.create_task(
                _feature_worker(states[s], feature_engine, feature_out, stop_event,
                                settings.features.interval_s)
            )
            for s in symbols
        ]
        if notifier is not None:
            await notifier.start()
        engine_task = asyncio.create_task(
            _run_engine_guarded(engine, args, notifier)
        )
        metrics_task = asyncio.create_task(_metrics_summary(stop_event))
        ws = WebSocketManager(settings.ws, bus, METRICS)
        ws_task = asyncio.create_task(ws.run(symbols, stop_event))

        log.info("paper pipeline ready",
                 extra={"symbols": len(symbols),
                        "equity": settings.paper.start_equity,
                        "db": str(settings.paper.db_path)})
        await stop_event.wait()
        engine.stop()
    finally:
        stop_event.set()
        engine.stop()
        for t in [*workers, engine_task, metrics_task, ws_task]:
            if t is not None:
                t.cancel()
        await asyncio.gather(
            *[t for t in [*workers, engine_task, metrics_task, ws_task] if t is not None],
            return_exceptions=True,
        )
        if notifier is not None:
            try:
                await notifier.stop()
            except Exception:  # noqa: BLE001 - shutdown debe ser best-effort
                log.warning("notifier stop failed", exc_info=True)
        for proc in processors:
            await proc.close()
        await rest.close()
    return 0


async def _run_engine_guarded(
    engine: PaperTradingEngine,
    args: argparse.Namespace,
    notifier: Optional[TelegramNotifier],
) -> None:
    """Run del motor con alerta CRITICAL_ERROR en caso de crash fatal."""
    try:
        await engine.run(max_trades=args.trades)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - cualquier crash fatal se reporta
        log.exception("paper engine crashed")
        if notifier is not None:
            notifier.notify_critical_error(f"{type(exc).__name__}: {exc}")
        raise


async def _snapshot_source(queue: asyncio.Queue, stop_event: asyncio.Event):
    """Adapt the feature queue into an AsyncIterator[FeatureSnapshot]."""
    while not stop_event.is_set():
        try:
            snapshot: FeatureSnapshot = await asyncio.wait_for(queue.get(), timeout=0.5)
            yield snapshot
        except asyncio.TimeoutError:
            continue


def _build_observability_repository(
    repository: Optional[Repository],
    settings: Settings,
    *,
    mode: str,
    alerts: bool,
    start_equity: float,
) -> tuple:
    """Envuelve el repo SQLite con el observador (alerts + heartbeat).

    Devuelve (repo_observado, notifier). El notifier queda SIN start() aquí;
    el caller lo arranca/para alrededor del ciclo del motor. Con $ None (o sin
    observer) devuelve (None|repo, None) para no cambiar comportamiento.
    """
    mon = settings.monitoring
    if repository is None:
        return None, None

    notifier = TelegramNotifier(
        mon.bot_token,
        mon.chat_id,
        enabled=bool(alerts) and mon.telegram_enabled,
        http_timeout_s=mon.http_timeout_s,
        queue_maxsize=mon.queue_size,
    )
    wrapped = ObservabilityRepository(
        repository,
        notifier=notifier,
        mode=mode,
        start_equity=start_equity,
        heartbeat_interval_ms=max(1, int(mon.heartbeat_interval_s * 1000)),
        halt_cooldown_ms=max(1, int(mon.alert_cooldown_s * 1000)),
    )
    return wrapped, notifier


async def _feature_worker(
    state: SymbolState,
    engine: FeatureEngine,
    out_q: asyncio.Queue,
    stop_event: asyncio.Event,
    interval_s: float,
) -> None:
    while not stop_event.is_set():
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
        await asyncio.sleep(interval_s)


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


# ── Backtest (FASE 8) ────────────────────────────────────────────────────────


async def run_backtest(settings: Settings, args: argparse.Namespace) -> int:
    """Replay histórico kline por el stack real: Features → Señal → Risk → Ejecución.

    El corpus viene del store CSV local (data/klines) o, si falta, se descarga
    paginado y se cachea. Sin clave API: solo datos públicos de klines.
    """
    from crypto_scalper.backtest.engine import BacktestEngine
    from crypto_scalper.ml.predictor import MLPredictor
    from crypto_scalper.strategies.signal_engine import SignalEngine

    bt = settings.backtest
    news_store = NewsStore()
    strategy = dataclasses.replace(settings.strategies, enabled=True)
    signal_engine = SignalEngine(strategy)
    predictor = _maybe_load_predictor(bt.model_path)

    try:
        corpus = await _resolve_backtest_corpus(settings, args)
    except (ExchangeConnectionError, ConnectionError, ValueError) as exc:
        log.error("backtest corpus unavailable", extra={"error": str(exc)})
        return 1
    if corpus is None:
        log.error("no klines in the requested window; use --start/--end")
        return 1

    feature_engine = FeatureEngine(settings.features, news_store, METRICS, predictor=predictor)
    repository: Optional[Repository] = None
    if bt.db_path:
        repository, _ = _build_observability_repository(
            SqliteRepository(bt.db_path),
            settings,
            mode="backtest",
            alerts=False,
            start_equity=settings.backtest.start_equity,
        )

    engine = BacktestEngine(
        settings=settings,
        corpus=corpus,
        feature_engine=feature_engine,
        signal_engine=signal_engine,
        predictor=predictor,
        news_store=news_store,
        repository=repository,
    )
    try:
        report = await engine.run(max_trades=args.trades or 0)
    finally:
        if repository is not None:
            await repository.close()

    print(report.summary_text())
    print(json.dumps(report.to_dict(), indent=2))
    log.info("backtest finished", extra={"trades": report.stats["trades"]})
    return 0


def _maybe_load_predictor(path: Path) -> Optional["MLPredictor"]:
    from crypto_scalper.ml.predictor import MLPredictor
    if not Path(path).exists():
        return None
    try:
        loaded = MLPredictor.load(path)
        log.info("ml predictor loaded", extra={"version": loaded.model_version})
        return loaded
    except Exception as exc:  # noqa: BLE001 - optional ML is never fatal
        log.warning("ml predictor failed to load; running without ml",
                    extra={"error": str(exc)})
        return None


async def _resolve_backtest_corpus(settings: Settings, args: argparse.Namespace):
    """Carga el corpus del store CSV; si falta/está fuera de rango, descarga."""
    bt = settings.backtest
    symbols = settings.explicit_symbols or ["BTCUSDT"]
    start_ms, end_ms = _parse_backtest_window(args)
    candles_by_symbol: dict = {}
    rest: Optional[BinanceFuturesRest] = None
    try:
        for symbol in symbols:
            path = klines_csv_path(bt.data_dir, symbol, bt.interval_s)
            span: list = []
            if path.exists():
                loaded = load_klines_csv(path, symbol, bt.interval_s)
                span = [c for c in loaded if start_ms <= c.ts_ms <= end_ms]
            if not span:
                if path.exists():
                    loaded_existing = load_klines_csv(path, symbol, bt.interval_s)
                    log.info("cache out of range; refreshing",
                             extra={"symbol": symbol, "path": str(path)})
                if rest is None:
                    rest = BinanceFuturesRest(settings.rest_url)
                    await rest.start()
                downloader = BinanceKlinesDownloader(rest, interval_s=bt.interval_s)
                fetched = await downloader.download(symbol, start_ms, end_ms)
                if fetched:
                    merged = list(fetched)
                    if path.exists():
                        known = {c.ts_ms for c in loaded_existing}
                        merged = loaded_existing + [c for c in fetched if c.ts_ms not in known]
                    save_klines_csv(path, merged)
                    span = [c for c in fetched if start_ms <= c.ts_ms <= end_ms]
            if span:
                candles_by_symbol[symbol] = span
        if not candles_by_symbol:
            return None
        return KlineCorpus(candles_by_symbol)
    finally:
        if rest is not None:
            await rest.close()


def _parse_backtest_window(args: argparse.Namespace):
    """--start/--end (UTC, ISO) → (start_ms, end_ms). Defaults: last 90 días."""
    end_ms = _iso_to_ms(args.end, default=int(datetime.now(timezone.utc).timestamp() * 1000))
    start_ms = _iso_to_ms(args.start, default=end_ms - 90 * 86400 * 1000)
    if start_ms >= end_ms:
        raise ValueError("--start must be earlier than --end")
    return start_ms, end_ms


def _iso_to_ms(value: str, default: int) -> int:
    if not value or not value.strip():
        return default
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


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