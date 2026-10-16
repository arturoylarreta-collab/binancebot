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
import time
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
from crypto_scalper.core import clock
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
    p.add_argument("--venue", default=None, choices=["paper", "testnet"],
                   help="paper mode execution venue (default: EXECUTION_VENUE)")
    return p.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    if args.env:
        import os
        os.environ["APP_ENV"] = args.env  # Settings.load() applies the matching .env file

    settings = Settings.load()
    if args.symbols:
        settings = _override_symbols(settings, args.symbols)
    log_dir = Path(args.log_dir) if args.log_dir else settings.log_dir
    setup_logging(level=settings.log_level, log_dir=log_dir,
                  fmt=settings.log_format, enable_file=_as_bool_env("LOG_TO_FILE", True))
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


def _as_bool_env(name: str, default: bool) -> bool:
    import os
    raw = os.environ.get(name, "")
    return default if raw == "" else raw.strip().lower() in ("1", "true", "yes", "on")


def _override_symbols(settings: Settings, raw: str) -> Settings:
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    import dataclasses
    return dataclasses.replace(settings, explicit_symbols=symbols)


# ── Shared live data plane ────────────────────────────────────────────────────


class _MarketData:
    """WebSocket → processors → SymbolState → FeatureEngine → snapshot queue.

    Start order matters: the WebSocket streams first so depth diffs buffer
    while each processor takes its REST snapshot (Binance sync protocol).
    """

    def __init__(self, settings: Settings, rest: BinanceFuturesRest, symbols: List[str],
                 stop_event: asyncio.Event, predictor=None) -> None:
        self.settings = settings
        self.symbols = symbols
        self.stop_event = stop_event
        self.bus = EventBus(queue_maxsize=max(1000, settings.ws.event_queue_maxsize // 10))
        self.states = {s: SymbolState(s) for s in symbols}
        self.processors = [
            SymbolProcessor(symbol=s, state=self.states[s], rest=rest, bus=self.bus,
                            metrics=METRICS, depth_snapshot_limit=settings.ws.depth_snapshot_limit,
                            depth_mode=settings.ws.depth_mode)
            for s in symbols
        ]
        self.features = FeatureEngine(settings.features, NewsStore(), METRICS, predictor=predictor)
        self.out: asyncio.Queue = asyncio.Queue(maxsize=max(100, 20 * len(symbols)))
        self.ws = WebSocketManager(settings.ws, self.bus, METRICS)
        self.tasks: List[asyncio.Task] = []

    def latest_price(self, symbol: str) -> Optional[float]:
        st = self.states.get(symbol)
        return st.latest_price if st is not None else None

    async def start(self) -> None:
        for proc in self.processors:
            await proc.start()           # subscribe only (no snapshot yet)
        self.tasks.append(asyncio.create_task(self.ws.run(self.symbols, self.stop_event), name="ws"))
        await asyncio.sleep(1.0)         # let the diff stream start buffering
        for proc in self.processors:
            self.tasks.append(asyncio.create_task(proc.run(self.stop_event), name=f"proc-{proc.symbol}"))
        for sym in self.symbols:
            self.tasks.append(asyncio.create_task(
                _feature_worker(self.states[sym], self.features, self.out, self.stop_event,
                                self.settings.features.interval_s),
                name=f"features-{sym}"))

    async def close(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for proc in self.processors:
            await proc.close()


async def _supervise(tasks: dict, stop_event: asyncio.Event) -> int:
    """Wait until stop is requested or ANY critical task ends.

    A critical task ending on its own is a failure: return non-zero so the
    platform (Render/Docker restart policy) restarts the process instead of
    leaving a zombie that serves a healthy-looking dashboard.
    """
    stop_task = asyncio.create_task(stop_event.wait(), name="stop")
    waiting = set(tasks.values()) | {stop_task}
    done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
    rc = 0
    for t in done:
        if t is stop_task:
            continue
        name = next((k for k, v in tasks.items() if v is t), t.get_name())
        if t.cancelled():
            log.error("critical task cancelled", extra={"task": name})
            rc = 1
        elif t.exception() is not None:
            log.critical("critical task crashed", extra={"task": name},
                         exc_info=t.exception())
            rc = 1
        else:
            log.warning("critical task finished", extra={"task": name})
            rc = rc or (0 if name == "engine" else 1)
    stop_event.set()
    stop_task.cancel()
    return rc


# ── Full pipeline ───────────────────────────────────────────────────────────────


async def run_pipeline(settings: Settings) -> int:
    rest = BinanceFuturesRest(settings.rest_url)
    repo: Repository = NoopRepository()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    md: Optional[_MarketData] = None
    extra: List[asyncio.Task] = []
    try:
        await rest.start()
        symbols = await _resolve_universe(settings, rest)
        md = _MarketData(settings, rest, symbols, stop_event)
        await md.start()
        signal_engine = SignalEngine(settings.strategies)
        extra = [
            asyncio.create_task(_feature_sink(md.out, repo, signal_engine, stop_event), name="sink"),
            asyncio.create_task(_metrics_summary(stop_event), name="metrics"),
        ]
        log.info("pipeline ready", extra={"symbols": len(symbols)})
        rc = await _supervise({"sink": extra[0], "ws": md.tasks[0]}, stop_event)
    finally:
        stop_event.set()
        for t in extra:
            t.cancel()
        await asyncio.gather(*extra, return_exceptions=True)
        if md is not None:
            await md.close()
        await repo.close()
        await rest.close()
    return rc


# ── Paper / testnet trading ───────────────────────────────────────────────────


async def run_paper(settings: Settings, args: argparse.Namespace) -> int:
    """Real market data + execution on the configured venue, 24/7.

    ``EXECUTION_VENUE=paper`` (default) simulates fills; ``testnet`` sends real
    orders to the Binance Futures demo account. ``--trades N`` stops after N
    closed trades; without it the bot runs until SIGTERM. The embedded HTTP
    server exposes /healthz and the dashboard on $PORT.
    """
    from crypto_scalper.execution.filters import FilterRegistry
    from crypto_scalper.monitoring.runtime import RuntimeState
    from crypto_scalper.server.app import start_server

    venue_cfg = settings.venue
    venue = getattr(args, "venue", None) or venue_cfg.effective_venue()
    if venue == "testnet" and not venue_cfg.has_credentials:
        log.error("testnet venue requires BINANCE_TESTNET_API_KEY/SECRET")
        return 2
    rest = BinanceFuturesRest(settings.rest_url)
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    runtime = RuntimeState(venue=venue, requested_venue=venue_cfg.venue, mode="trading",
                           db_path=str(settings.paper.db_path))
    if venue_cfg.venue == "testnet" and venue != "testnet":
        runtime.note("EXECUTION_VENUE=testnet sin API keys: corriendo en paper")
        log.warning("testnet requested without credentials; running on paper")

    strategy = dataclasses.replace(settings.strategies, enabled=True)
    signal_engine = SignalEngine(strategy)
    repository, notifier = _build_observability_repository(
        SqliteRepository(settings.paper.db_path),
        settings,
        mode=venue,
        alerts=True,
        start_equity=settings.paper.start_equity,
    )

    runner = None
    mirror = None
    md: Optional[_MarketData] = None
    engine: Optional[PaperTradingEngine] = None
    tasks: dict = {}
    rc = 0
    try:
        # The dashboard/health endpoint comes up first so the platform sees
        # the service as starting even while the universe is resolved.
        runner = await start_server(settings, runtime)
        await rest.start()
        # Shared cloud IPs are sometimes REST-banned by Binance (HTTP 418):
        # public metadata falls back to the demo host; market data itself
        # comes from WebSocket streams, which need no REST at all.
        demo = BinanceFuturesRest(settings.venue.testnet_rest_url)
        await demo.start()
        try:
            for src in (rest, demo):
                try:
                    offset = await asyncio.wait_for(clock.sync_with(src), timeout=8)
                    log.info("clock synced with exchange", extra={"offset_ms": offset})
                    break
                except Exception as exc:  # noqa: BLE001 - fall back to host clock
                    log.warning("clock sync failed", extra={"error": repr(exc)[:160]})
            symbols = await _resolve_universe(settings, rest)
            runtime.symbols = list(symbols)

            filters = FilterRegistry()
            for src in (rest, demo, rest):
                try:
                    # Real exchange grid even on paper, so paper sizing == testnet sizing.
                    info = await asyncio.wait_for(src.exchange_info(), timeout=10)
                    filters = FilterRegistry.from_exchange_info({"symbols": info}, symbols)
                    if len(filters):
                        break
                except Exception as exc:  # noqa: BLE001 - paper can fall back to defaults
                    log.warning("exchange filters unavailable", extra={"error": repr(exc)[:160]})
            if not len(filters):
                runtime.note("filtros del exchange no disponibles: redondeo por defecto")
        finally:
            await demo.close()

        if venue == "testnet":
            from crypto_scalper.execution.binance_futures import preflight
            problem = await preflight(settings)
            if problem:
                log.error("testnet preflight failed; staying on paper", extra={"reason": problem})
                runtime.note(f"testnet no disponible ({problem}); corriendo en paper")
                venue = "paper"
                runtime.venue = "paper"
            else:
                runtime.note("testnet preflight OK: API keys válidas")

        mirror = await _start_mirror(settings, venue, runtime)

        md = _MarketData(settings, rest, symbols, stop_event)
        runtime.states = md.states
        runtime.ws_manager = md.ws
        engine = PaperTradingEngine(
            settings=settings,
            signal_engine=signal_engine,
            source=_snapshot_source(md.out, stop_event),
            repository=repository,
            venue=venue,
            filters=filters,
            symbols=symbols,
            price_source=md.latest_price,
            runtime=runtime,
            max_snapshot_age_ms=int(max(5.0, 5 * settings.features.interval_s) * 1000),
            mirror=mirror,
            flatten_on_shutdown=settings.durability.flatten_paper_on_shutdown,
        )
        if mirror is not None:
            await _restore_from_mirror(mirror, engine, repository, venue, runtime)
        await md.start()
        if notifier is not None:
            await notifier.start()
        tasks = {
            "engine": asyncio.create_task(_run_engine_guarded(engine, args, notifier, runtime),
                                          name="engine"),
            "ws": md.tasks[0],
        }
        metrics_task = asyncio.create_task(_metrics_summary(stop_event, rest), name="metrics")
        keepalive_task = None
        runtime.keepalive = _KEEPALIVE_STATE
        if settings.durability.keepalive_url:
            keepalive_task = asyncio.create_task(
                _keepalive(settings.durability.keepalive_url,
                           settings.durability.keepalive_interval_s, stop_event),
                name="keepalive")
        log.info("trading pipeline ready",
                 extra={"venue": venue, "symbols": ",".join(symbols),
                        "equity": settings.paper.start_equity,
                        "db": str(settings.paper.db_path),
                        "port": settings.server.port})
        runtime.note(f"arranque: venue={venue} símbolos={','.join(symbols)}")
        rc = await _supervise(tasks, stop_event)
        metrics_task.cancel()
        if keepalive_task is not None:
            keepalive_task.cancel()
    except Exception:  # noqa: BLE001 - startup failure must exit non-zero
        log.exception("trading pipeline failed to start")
        rc = 1
    finally:
        stop_event.set()
        if engine is not None:
            engine.stop()
        for t in tasks.values():
            t.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        if notifier is not None:
            try:
                await asyncio.wait_for(notifier.stop(), timeout=5.0)
            except Exception:  # noqa: BLE001 - shutdown debe ser best-effort
                log.warning("notifier stop failed", exc_info=True)
        if md is not None:
            await md.close()
        await rest.close()
        if mirror is not None:
            await mirror.close()
        if runner is not None:
            await runner.cleanup()
    return rc


async def _start_mirror(settings: Settings, venue: str, runtime):
    """Firestore (Firebase Spark) durable mirror; None when not configured."""
    dur = settings.durability
    if not dur.firestore_enabled:
        return None
    from crypto_scalper.storage.firestore_mirror import FirestoreMirror

    mirror = FirestoreMirror.from_env(
        dur.firebase_service_account, bot_id=dur.bot_id or venue,
        equity_interval_s=dur.equity_interval_s, flush_interval_s=dur.flush_interval_s)
    if mirror is None:
        runtime.note("Firestore: credenciales inválidas; sin persistencia durable")
        return None
    try:
        await mirror.start()
    except Exception as exc:  # noqa: BLE001 - durability is optional, trading is not
        log.error("firestore unavailable", extra={"error": repr(exc)})
        runtime.note(f"Firestore no disponible: {type(exc).__name__}")
        await mirror.close(timeout_s=1.0)
        return None
    runtime.mirror = mirror
    return mirror


async def _restore_from_mirror(mirror, engine, repository, venue: str, runtime) -> None:
    """Resume account + history after a restart on an ephemeral filesystem."""
    from crypto_scalper.core.enums import PositionStatus
    from crypto_scalper.core.models import HeartbeatSnapshot, ManagedPosition

    try:
        state = await mirror.load_state()
        if state:
            engine.restore_state(state)
        inner = getattr(repository, "inner", repository)
        if hasattr(inner, "count_rows") and inner.count_rows("positions") == 0:
            since = int(time.time() * 1000) - 7 * 86_400_000
            trades = await mirror.load_collection("trades", order_field="closed_ts_ms",
                                                  since=since, limit=500)
            for t in trades:
                await inner.save_position(ManagedPosition(
                    position_id=t["position_id"], symbol=t["symbol"], side=t["side"],
                    quantity=float(t["quantity"]), ordered_quantity=float(t["quantity"]),
                    entry_price=float(t["entry_price"]),
                    stop_loss_price=float(t.get("stop_loss_price", 0.0)),
                    take_profit_price=float(t.get("take_profit_price", 0.0)),
                    notional_value=float(t.get("notional_value", 0.0)),
                    risk_amount=float(t.get("risk_amount", 0.0)),
                    regime=str(t.get("regime", "")), status=PositionStatus.CLOSED,
                    entry_client_order_id=f"ENTRY-{t['position_id'].upper()}",
                    opened_ts_ms=int(t.get("opened_ts_ms", 0)),
                    closed_ts_ms=int(t.get("closed_ts_ms", 0)),
                    realized_pnl=float(t.get("realized_pnl", 0.0)),
                    close_reason=str(t.get("close_reason", "")),
                    fees=float(t.get("fees", 0.0)), exit_price=float(t.get("exit_price", 0.0)),
                ))
            points = await mirror.load_collection("equity", order_field="ts_ms",
                                                  since=since, limit=2100)
            for p in points:
                await inner.save_heartbeat(HeartbeatSnapshot(
                    ts_ms=int(p["ts_ms"]), mode=venue, status="restored",
                    equity=float(p["equity"]), realized_pnl=float(p.get("realized", 0.0)),
                    unrealized_pnl=float(p.get("unrealized", 0.0)),
                    drawdown_pct=float(p.get("drawdown_pct", 0.0)), total_exposure=0.0,
                    open_count=int(p.get("open_count", 0)), trades_today=0, uptime_ms=0))
            runtime.note(f"restaurado de Firestore: {len(trades)} trades, {len(points)} puntos de equity")
        elif state:
            runtime.note("estado restaurado de Firestore")
    except Exception as exc:  # noqa: BLE001 - a failed restore must not block trading
        log.exception("firestore restore failed")
        runtime.note(f"restauración Firestore falló: {type(exc).__name__}")


_KEEPALIVE_STATE: dict = {"target": "", "last_ms": 0, "last_status": None}


async def _keepalive(url: str, interval_s: float, stop_event: asyncio.Event) -> None:
    """Self-ping the public URL so free hosts that idle-sleep keep the bot up."""
    import aiohttp

    target = url.rstrip("/") + "/healthz"
    _KEEPALIVE_STATE["target"] = target
    log.info("keepalive enabled", extra={"target": target, "interval_s": interval_s})
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
                return
            except asyncio.TimeoutError:
                pass
            try:
                async with session.get(target) as resp:
                    METRICS.incr("keepalive.ok" if resp.status < 500 else "keepalive.bad")
                    _KEEPALIVE_STATE.update(last_ms=int(time.time() * 1000), last_status=resp.status)
            except Exception as exc:  # noqa: BLE001
                METRICS.incr("keepalive.error")
                log.debug("keepalive failed", extra={"error": repr(exc)})


async def _run_engine_guarded(
    engine: PaperTradingEngine,
    args: argparse.Namespace,
    notifier: Optional[TelegramNotifier],
    runtime=None,
) -> None:
    """Run del motor con alerta CRITICAL_ERROR en caso de crash fatal."""
    try:
        await engine.run(max_trades=args.trades)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - cualquier crash fatal se reporta
        log.exception("trading engine crashed")
        if runtime is not None:
            runtime.engine_error = f"{type(exc).__name__}: {exc}"[:300]
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
    el caller lo arranca/para alrededor del ciclo del motor.
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


_MAX_DATA_AGE_MS = 15_000


async def _feature_worker(
    state: SymbolState,
    engine: FeatureEngine,
    out_q: asyncio.Queue,
    stop_event: asyncio.Event,
    interval_s: float,
) -> None:
    """Emit one snapshot per interval while the symbol's data is fresh.

    A stale or unsynchronized symbol emits nothing (never trade on a frozen
    book); a full queue drops the OLDEST snapshot, keeping decisions current.
    """
    loop = asyncio.get_running_loop()
    next_t = loop.time()
    while not stop_event.is_set():
        try:
            now_ms = clock.now_ms()
            if state.is_ready() and state.data_age_ms(now_ms) <= _MAX_DATA_AGE_MS:
                snapshot = engine.compute(state, now_ms=now_ms)
                if out_q.full():
                    try:
                        out_q.get_nowait()
                        METRICS.incr("features.dropped_oldest")
                    except asyncio.QueueEmpty:
                        pass
                out_q.put_nowait(snapshot)
            elif state.orderbook.has_snapshot:
                METRICS.incr(f"features.{state.symbol}.not_fresh")
        except MarketDataNotReady:
            pass  # still warming up; normal before the first candles exist
        except Exception:  # noqa: BLE001
            log.exception("feature worker error", extra={"symbol": state.symbol})
        next_t += interval_s
        await asyncio.sleep(max(0.0, next_t - loop.time()))
        if loop.time() - next_t > 5 * interval_s:
            next_t = loop.time()  # we fell behind (e.g. GC/CPU spike): resync cadence


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


async def _metrics_summary(stop_event: asyncio.Event, rest=None) -> None:
    n = 0
    while not stop_event.is_set():
        await asyncio.sleep(60.0)
        n += 1
        if rest is not None and n % 10 == 0:
            try:
                await clock.sync_with(rest)
            except Exception:  # noqa: BLE001
                pass
        snap = METRICS.snapshot()
        counters = {k[len("counter."):]: v for k, v in snap.items() if k.startswith("counter.")}
        log.info("metrics summary", extra={"metrics": counters})


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