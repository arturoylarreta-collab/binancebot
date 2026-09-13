"""Paper trading engine (FASE 7).

Unifies every non-live layer into a single runnable unit:

    snapshot source → Signal Engine → TradeOrchestrator → venue (simulated)

`PaperTradingEngine` owns the execution stack, the periodic reconciliation,
the summary cadence and the one-shot trade cap.  The caller only has to
supply an `AsyncIterator[FeatureSnapshot]`; the rest is fully deterministic
and offline.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, Optional

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.paper import PaperConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.execution.execution_router import ExecutionRouter, build_execution_stack
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.base import NoopRepository, Repository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.trading.account import PaperAccount
from crypto_scalper.trading.orchestrator import TradeOrchestrator
from crypto_scalper.trading.reconciler import PeriodicReconciler

log = logging.getLogger(__name__)


class PaperTradingEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        signal_engine: SignalEngine,
        source: AsyncIterator[FeatureSnapshot],
        repository: Optional[Repository] = None,
    ) -> None:
        self._settings = settings
        self._signals = signal_engine
        self._source = source
        self._repo = repository or NoopRepository()

        risk = RiskEngine(settings.risk)
        adapter, om, pm, router = build_execution_stack("paper", settings, risk)
        self._adapter = adapter
        self._om = om
        self._pm = pm
        self._router = router

        account = PaperAccount(
            settings.paper.start_equity,
            settings.paper.fee_pct,
        )
        reconciler = PeriodicReconciler(
            pm, om, adapter,
            interval_s=settings.paper.reconcile_interval_s,
            repository=self._repo,
        )
        self._orchestrator = TradeOrchestrator(
            router, signal_engine, account, self._repo,
            order_manager=om, reconciler=reconciler,
            config=settings.paper,
        )
        self._wire_observability()
        self._stop_event = asyncio.Event()

    @property
    def orchestrator(self) -> TradeOrchestrator:
        return self._orchestrator

    def _wire_observability(self) -> None:
        """Hooks duck-typed: solo se activan si el repo es un observador."""
        attach_mark = getattr(self._repo, "attach_mark", None)
        if attach_mark is not None:
            attach_mark(self._adapter.get_price)
        set_mode = getattr(self._repo, "set_mode", None)
        if set_mode is not None:
            set_mode("paper")

    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    async def run(self, *, max_trades: Optional[int] = None) -> int:
        await self._orchestrator.start()
        summary_s = self._settings.paper.summary_interval_s
        last_summary = asyncio.get_running_loop().time()
        try:
            async for snapshot in self._source:
                if self._stop_event.is_set():
                    break
                await self._orchestrator.on_price(snapshot.symbol, snapshot.price)
                reached_max = (max_trades is not None
                               and self._orchestrator.trades_closed >= max_trades)
                if not reached_max:
                    signal = self._signals.evaluate(snapshot)
                    await self._orchestrator.on_signal(signal, snapshot)
                await self._orchestrator.persist()
                METRICS.incr("paper.snapshots_processed")
                if reached_max:
                    log.info("one-shot trade limit reached",
                             extra={"max_trades": max_trades})
                    break
                now = asyncio.get_running_loop().time()
                if summary_s > 0 and now - last_summary >= summary_s:
                    self._orchestrator.log_summary()
                    last_summary = now
        finally:
            self._orchestrator.log_summary()
            await self._orchestrator.close()
            await self._repo.close()
        self._stop_event.set()
        return 0

    def stop(self) -> None:
        self._stop_event.set()