"""Trading engine (FASE 7 paper → FASE 9 paper/testnet).

Unifies every execution layer into a single runnable unit:

    snapshot source → Signal Engine → TradeOrchestrator → venue

The venue is the offline simulator (``paper``) or Binance Futures testnet
(``testnet``, real orders on a trial account). Everything above the adapter
is identical, so a strategy validated on paper runs unchanged on testnet.

Built to run unattended 24/7:
  * one bad snapshot never kills the loop (logged, counted, skipped);
  * stale snapshots are dropped instead of trading on old prices;
  * persistence is incremental and throttled; memory is pruned periodically;
  * ``max_trades=None`` runs forever, ``N`` stops after N closed trades.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator, Callable, List, Optional

from crypto_scalper.config.settings import Settings
from crypto_scalper.core import clock
from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.execution.execution_router import build_execution_stack
from crypto_scalper.execution.filters import FilterRegistry
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.monitoring.runtime import RuntimeState
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.base import NoopRepository, Repository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.trading.account import PaperAccount, VenueAccount
from crypto_scalper.trading.orchestrator import TradeOrchestrator
from crypto_scalper.trading.reconciler import PeriodicReconciler

log = logging.getLogger(__name__)

_MAX_CONSECUTIVE_ERRORS = 50
_PERSIST_EVERY_S = 1.0
_PRUNE_EVERY_S = 600.0
_DB_RETENTION_EVERY_S = 3600.0


class PaperTradingEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        signal_engine: SignalEngine,
        source: AsyncIterator[FeatureSnapshot],
        repository: Optional[Repository] = None,
        venue: str = "paper",
        filters: Optional[FilterRegistry] = None,
        symbols: Optional[List[str]] = None,
        price_source: Optional[Callable[[str], Optional[float]]] = None,
        runtime: Optional[RuntimeState] = None,
        max_snapshot_age_ms: Optional[int] = None,
        mirror=None,
        flatten_on_shutdown: bool = False,
    ) -> None:
        self._mirror = mirror
        self._flatten_on_shutdown = flatten_on_shutdown
        self._settings = settings
        self._signals = signal_engine
        self._source = source
        self._repo = repository or NoopRepository()
        self._venue = venue
        self._runtime = runtime or RuntimeState(venue=venue)
        # Wall-clock staleness guard for live feeds (None = off, e.g. replays).
        self._max_snapshot_age_ms = max_snapshot_age_ms

        risk = RiskEngine(settings.risk)
        adapter, om, pm, router = build_execution_stack(
            "paper", settings, risk, venue=venue, filters=filters,
            symbols=symbols, price_source=price_source,
        )
        self._adapter = adapter
        self._om = om
        self._pm = pm
        self._router = router

        if venue == "testnet":
            account: PaperAccount = VenueAccount(settings.paper.start_equity,
                                                 settings.paper.fee_pct)
        else:
            account = PaperAccount(settings.paper.start_equity, settings.paper.fee_pct)
        self._account = account
        reconciler = PeriodicReconciler(
            pm, om, adapter,
            interval_s=settings.paper.reconcile_interval_s,
            repository=self._repo,
            set_paused=lambda reason: self._orchestrator.pause(reason),
            managed_symbols=set(symbols) if symbols else None,
        )
        self._orchestrator = TradeOrchestrator(
            router, signal_engine, account, self._repo,
            order_manager=om, reconciler=reconciler,
            config=settings.paper,
        )
        self._runtime.orchestrator = self._orchestrator
        self._runtime.adapter = adapter
        if self._mirror is not None:
            pm.on_position_closed(self._mirror_trade)
        self._wire_observability()
        self._stop_event = asyncio.Event()

    @property
    def orchestrator(self) -> TradeOrchestrator:
        return self._orchestrator

    @property
    def adapter(self):
        return self._adapter

    @property
    def venue(self) -> str:
        return self._venue

    def _wire_observability(self) -> None:
        """Hooks duck-typed: solo se activan si el repo es un observador."""
        attach_mark = getattr(self._repo, "attach_mark", None)
        if attach_mark is not None:
            attach_mark(self._adapter.get_price)
        set_mode = getattr(self._repo, "set_mode", None)
        if set_mode is not None:
            set_mode(self._venue)
        attach_equity = getattr(self._repo, "attach_equity", None)
        if attach_equity is not None:
            attach_equity(self._equity_view)

    def _equity_view(self) -> dict:
        s = self._orchestrator.summary()
        status = "paused" if s.paused else ("halted" if s.trading_halted else "running")
        return {"equity": s.equity, "realized_pnl": s.realized_pnl - s.fees_paid,
                "unrealized_pnl": s.unrealized_pnl, "status": status}

    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    async def run(self, *, max_trades: Optional[int] = None) -> int:
        if self._venue == "testnet":
            await self._adapter.start()
            self._sync_venue_account(initial=True)
        await self._orchestrator.start()
        loop = asyncio.get_running_loop()
        summary_s = self._settings.paper.summary_interval_s
        last_summary = last_persist = last_prune = last_retention = loop.time()
        last_state = last_equity = 0.0
        consecutive_errors = 0
        self._runtime.engine_alive = True
        try:
            async for snapshot in self._source:
                if self._stop_event.is_set():
                    break
                try:
                    reached = await self._step(snapshot, max_trades)
                    consecutive_errors = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - keep trading on bad ticks
                    consecutive_errors += 1
                    self._runtime.loop_errors += 1
                    self._runtime.last_error = f"{type(exc).__name__}: {exc}"[:300]
                    METRICS.incr("engine.loop_errors")
                    log.exception("engine step failed", extra={"symbol": snapshot.symbol})
                    if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                        raise
                    continue
                now = loop.time()
                if now - last_persist >= _PERSIST_EVERY_S or reached:
                    await self._orchestrator.persist(refresh=self._venue == "paper")
                    last_persist = now
                if self._mirror is not None:
                    if now - last_state >= 60.0:
                        self._mirror.put_state(self.export_state())
                        last_state = now
                    if now - last_equity >= self._mirror.equity_interval_s:
                        self._mirror_equity()
                        last_equity = now
                if now - last_prune >= _PRUNE_EVERY_S:
                    self._pm.prune_closed()
                    last_prune = now
                if now - last_retention >= _DB_RETENTION_EVERY_S:
                    prune = getattr(getattr(self._repo, "inner", self._repo), "prune", None)
                    if callable(prune):
                        prune()
                    last_retention = now
                if reached:
                    log.info("trade limit reached", extra={"max_trades": max_trades})
                    break
                if summary_s > 0 and now - last_summary >= summary_s:
                    self._orchestrator.log_summary()
                    last_summary = now
        finally:
            self._runtime.engine_alive = False
            if self._flatten_on_shutdown and self._venue == "paper":
                # Simulated positions cannot survive a restart: realize them at
                # the last price so the resumed account keeps their PnL.
                try:
                    closed = await self._orchestrator.flatten_all("shutdown")
                    if closed:
                        log.info("paper positions closed on shutdown", extra={"count": closed})
                except Exception:  # noqa: BLE001
                    log.exception("shutdown flatten failed")
            if self._mirror is not None:
                self._mirror.put_state(self.export_state())
                self._mirror_equity()
            try:
                await self._orchestrator.persist(refresh=False)
            except Exception:  # noqa: BLE001
                log.exception("final persist failed")
            self._orchestrator.log_summary()
            await self._orchestrator.close()
            if self._venue == "testnet":
                await self._adapter.close()
            await self._repo.close()
        self._stop_event.set()
        return 0

    async def _step(self, snapshot: FeatureSnapshot, max_trades: Optional[int]) -> bool:
        age_ms = clock.now_ms() - snapshot.timestamp_ms
        if self._max_snapshot_age_ms is not None and age_ms > self._max_snapshot_age_ms:
            self._runtime.stale_snapshots_dropped += 1
            METRICS.incr("engine.stale_snapshot_dropped")
            return False
        self._runtime.last_snapshot_ms[snapshot.symbol] = snapshot.timestamp_ms
        self._runtime.last_price[snapshot.symbol] = snapshot.price
        if self._venue == "testnet":
            self._sync_venue_account()
        await self._orchestrator.on_price(snapshot.symbol, snapshot.price)
        reached = max_trades is not None and self._orchestrator.trades_closed >= max_trades
        if not reached:
            signal = self._signals.evaluate(snapshot)
            await self._orchestrator.on_signal(signal, snapshot)
        self._runtime.snapshots_processed += 1
        METRICS.incr("paper.snapshots_processed")
        tick = getattr(self._repo, "tick", None)
        if tick is not None:
            await tick()
        return reached

    def _sync_venue_account(self, initial: bool = False) -> None:
        snap = getattr(self._adapter, "account_snapshot", None)
        if not callable(snap) or not isinstance(self._account, VenueAccount):
            return
        acct = snap()
        wallet = float(acct.get("wallet", 0.0))
        if wallet <= 0:
            return
        if initial:
            # risk limits and drawdown are measured from the real starting wallet
            self._account._start_equity = wallet
            self._account._peak_equity = wallet + float(acct.get("unrealized", 0.0))
            set_start = getattr(self._repo, "set_start_equity", None)
            if callable(set_start):
                set_start(wallet)
        self._account.sync(wallet, float(acct.get("unrealized", 0.0)))

    def stop(self) -> None:
        self._stop_event.set()

    # ── durable state (FirestoreMirror) ──────────────────────────────────────

    def export_state(self) -> dict:
        acct = self._account
        s = self._orchestrator.summary()
        return {
            "venue": self._venue,
            "start_equity": acct.start_equity,
            "cash": acct.cash,
            "realized_pnl": acct.realized_pnl,
            "fees_paid": acct.fees_paid,
            "peak_equity": acct.peak_equity,
            "equity": s.equity,
            "trades_closed": self._orchestrator.trades_closed,
            "daily_realized_pnl": self._pm.daily_realized_pnl,
            "day": getattr(self._pm, "_day", ""),
            "consecutive_losses": self._pm.consecutive_losses,
            "last_loss_ms": getattr(self._pm, "_last_loss_ms", 0),
        }

    def restore_state(self, state: dict) -> None:
        """Resume counters (and, on paper, the whole account) from a mirror."""
        if not state:
            return
        if self._venue == "paper" and state.get("venue", "paper") == "paper":
            self._account.restore(
                cash=float(state.get("cash", self._account.cash)),
                realized_pnl=float(state.get("realized_pnl", 0.0)),
                fees_paid=float(state.get("fees_paid", 0.0)),
                peak_equity=float(state.get("peak_equity", self._account.peak_equity)),
                start_equity=float(state.get("start_equity", 0.0)),
            )
            peak = getattr(self._repo, "set_peak_equity", None)
            if callable(peak):
                peak(float(state.get("peak_equity", 0.0)))
            start = getattr(self._repo, "set_start_equity", None)
            if callable(start):
                start(float(state.get("start_equity", self._account.start_equity)))
        self._orchestrator._trades_closed = int(state.get("trades_closed", 0))
        if state.get("day") == getattr(self._pm, "_day", None):
            self._pm._daily_realized_pnl = float(state.get("daily_realized_pnl", 0.0))
        self._pm._consecutive_losses = int(state.get("consecutive_losses", 0))
        self._pm._last_loss_ms = int(state.get("last_loss_ms", 0))
        log.info("session state restored", extra={
            "cash": round(self._account.cash, 2), "trades": self._orchestrator.trades_closed})

    def _mirror_trade(self, pos) -> None:
        try:
            self._mirror.put_trade({
                "position_id": pos.position_id, "symbol": pos.symbol, "side": pos.side,
                "quantity": pos.quantity, "entry_price": pos.entry_price,
                "exit_price": getattr(pos, "exit_price", 0.0),
                "stop_loss_price": pos.stop_loss_price, "take_profit_price": pos.take_profit_price,
                "notional_value": pos.notional_value, "risk_amount": pos.risk_amount,
                "realized_pnl": pos.realized_pnl, "fees": getattr(pos, "fees", 0.0),
                "close_reason": pos.close_reason, "regime": pos.regime,
                "opened_ts_ms": pos.opened_ts_ms, "closed_ts_ms": pos.closed_ts_ms,
                "venue": self._venue,
            })
        except Exception:  # noqa: BLE001 - durability must never break trading
            log.exception("mirror trade failed")

    def _mirror_equity(self) -> None:
        s = self._orchestrator.summary()
        self._mirror.put_equity(int(time.time() * 1000), s.equity, s.drawdown_pct,
                                s.unrealized_pnl, s.open_count, s.realized_pnl - s.fees_paid)


TradingEngine = PaperTradingEngine
