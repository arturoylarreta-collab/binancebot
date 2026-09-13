"""TradeOrchestrator (FASE 7) — the offline trading loop.

Wires the exact same execution chain a live bot would use, minus the venue:

    Signal ─→ RiskEngine.assess() (via ExecutionRouter) ─→ PositionManager

Equity is fed from a PaperAccount (mark-to-market), every decision is
persisted for audit, and Fatal execution failures pause the bot instead of
letting it fire-and-forget into an inconsistent state.

Policies enforced here (on top of the Risk Engine's hard gates):
  * orchestrator pause (Fatal error or unresolved fatal reconciliation);
  * never open a second position on an already-open symbol (paper guard);
  * price ticks are pushed into the venue before a signal is evaluated, so
    fills always happen before the next decision is made.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.paper import PaperConfig
from crypto_scalper.core.enums import PositionStatus, RiskVerdict, SignalType
from crypto_scalper.core.models import ManagedPosition, RiskDecision, Signal
from crypto_scalper.execution.execution_router import ExecutionOutcome, ExecutionRouter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.storage.base import NoopRepository, Repository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.trading.account import PaperAccount

if TYPE_CHECKING:
    from crypto_scalper.trading.reconciler import PeriodicReconciler

log = logging.getLogger(__name__)

_FATAL_MARKERS = ("PositionNotProtectedError", "ProtectionTimeoutError")


def _is_fatal_error(error: Optional[str]) -> bool:
    if not error:
        return False
    return any(marker in error for marker in _FATAL_MARKERS)


@dataclass(frozen=True)
class TradeSummary:
    equity: float
    cash: float
    peak_equity: float
    drawdown_pct: float
    realized_pnl: float
    fees_paid: float
    unrealized_pnl: float
    open_count: int
    daily_realized_pnl: float
    trades_closed: int
    consecutive_losses: int
    trading_halted: bool
    safe_mode: bool
    paused: bool
    pause_reason: str = ""

    def to_dict(self) -> Dict[str, object]:
        return {
            "equity": round(self.equity, 6),
            "cash": round(self.cash, 6),
            "peak_equity": round(self.peak_equity, 6),
            "drawdown_pct": round(self.drawdown_pct, 6),
            "realized_pnl": round(self.realized_pnl, 6),
            "fees_paid": round(self.fees_paid, 6),
            "unrealized_pnl": round(self.unrealized_pnl, 6),
            "open_count": self.open_count,
            "daily_realized_pnl": round(self.daily_realized_pnl, 6),
            "trades_closed": self.trades_closed,
            "consecutive_losses": self.consecutive_losses,
            "trading_halted": self.trading_halted,
            "safe_mode": self.safe_mode,
            "paused": self.paused,
            "pause_reason": self.pause_reason,
        }


class TradeOrchestrator:
    def __init__(
        self,
        router: ExecutionRouter,
        signal_engine: SignalEngine,
        account: PaperAccount,
        repository: Optional[Repository] = None,
        *,
        order_manager: Optional[OrderManager] = None,
        reconciler: Optional["PeriodicReconciler"] = None,
        config: Optional[PaperConfig] = None,
    ) -> None:
        self._router = router
        self._pm = router.position_manager
        self._adapter = router.adapter
        self._signals = signal_engine
        self._account = account
        self._repo = repository or NoopRepository()
        self._om = order_manager
        self._reconciler = reconciler
        self._config = config or PaperConfig()

        self._paused = False
        self._pause_reason = ""
        self._trades_closed = 0
        self._closed: List[ManagedPosition] = []
        self._open_symbols: set = set()

        self._pm.on_position_closed(self._record_close)

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._pm.start()
        if self._reconciler is not None:
            await self._reconciler.start()

    async def close(self) -> None:
        if self._reconciler is not None:
            await self._reconciler.close()
        await self._router.close()

    # ── queries ──────────────────────────────────────────────────────────────

    @property
    def trades_closed(self) -> int:
        return self._trades_closed

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def pause_reason(self) -> str:
        return self._pause_reason

    @property
    def account(self) -> PaperAccount:
        return self._account

    @property
    def repository(self) -> Repository:
        return self._repo

    def closed_positions(self) -> Tuple[ManagedPosition, ...]:
        return tuple(self._closed)

    def open_positions(self) -> Tuple[ManagedPosition, ...]:
        return self._pm.open_positions()

    def build_portfolio(self) -> PortfolioState:
        unrealized = self._account.unrealized_pnl(
            self._pm.open_positions(), self._adapter.get_price
        )
        equity = self._account.equity(unrealized)
        return self._pm.build_portfolio_state(
            equity,
            initial_equity=self._account.start_equity,
            peak_equity=self._account.peak_equity,
        )

    # ── trading loop ─────────────────────────────────────────────────────────

    async def on_signal(self, signal: Signal, snapshot) -> ExecutionOutcome:
        METRICS.incr("paper.signals_seen")
        if self._paused:
            METRICS.incr("paper.blocked_paused")
            log.warning("signal blocked (orchestrator paused)",
                        extra={"symbol": signal.symbol, "reason": self._pause_reason})
            return await self._blocked(signal, f"orchestrator_paused:{self._pause_reason}")

        per_symbol = self._config.max_open_per_symbol
        if per_symbol and signal.symbol in self._open_symbols:
            METRICS.incr("paper.blocked_symbol_open")
            return await self._blocked(signal, f"symbol_already_open:{signal.symbol}")

        if signal.signal_type not in (SignalType.LONG, SignalType.SHORT):
            METRICS.incr("paper.blocked_flat")
            return await self._blocked(signal, "flat_signal_not_eligible")

        outcome = await self._router.route(signal, snapshot, self.build_portfolio())
        await self._repo.save_risk_decision(outcome.decision)
        await self._repo.save_signal(
            signal, self._signal_block_reason(outcome),
        )

        if outcome.submitted and outcome.position is not None:
            self._open_symbols.add(outcome.position.symbol)
            METRICS.incr("paper.trades_opened")
        elif outcome.decision.verdict != RiskVerdict.APPROVED.name:
            METRICS.incr("paper.trades_rejected")

        if _is_fatal_error(outcome.error):
            self._pause(outcome.error)
        return outcome

    async def on_price(self, symbol: str, price: float) -> None:
        """Push a market tick into the venue, let fills settle, mark to market."""
        self._router.update_price(symbol, price)
        await asyncio.sleep(0)
        self.build_portfolio()

    async def persist(self) -> None:
        """Persist the current positions/orders/fills for audit (idempotent)."""
        if self._om is not None:
            await self._om.refresh()
        for pos in self._pm.positions():
            await self._repo.save_position(pos)
        if self._om is not None:
            for report in self._om.orders():
                await self._repo.save_order(report)
                await self._repo.save_fills(report.fills)

    # ── accounting hooks ─────────────────────────────────────────────────────

    def _record_close(self, position: ManagedPosition) -> None:
        self._account.realize_close(position)
        self._open_symbols.discard(position.symbol)
        self._trades_closed += 1
        self._closed.append(position)
        self.build_portfolio()
        METRICS.incr("paper.trades_closed")
        log.info("paper trade closed", extra={
            "position_id": position.position_id,
            "symbol": position.symbol,
            "reason": position.close_reason,
            "pnl": round(position.realized_pnl, 6),
        })

    def _pause(self, reason: str) -> None:
        if self._paused:
            return
        self._paused = True
        self._pause_reason = reason
        METRICS.incr("paper.paused")
        log.critical("orchestrator paused", extra={"reason": reason})

    # ── bookkeeping pass-through ─────────────────────────────────────────────

    async def _blocked(self, signal: Signal, reason: str) -> ExecutionOutcome:
        await self._repo.save_signal(signal, rejection_reason=reason)
        return ExecutionOutcome(
            submitted=False,
            decision=self._blocked_decision(signal, reason),
            error=reason,
        )

    def _blocked_decision(self, signal: Signal, reason: str) -> RiskDecision:
        return RiskDecision(
            symbol=signal.symbol,
            ts_ms=int(time.time() * 1000),
            verdict=RiskVerdict.REJECTED.name,
            reason="other",
            verifier="trade_orchestrator",
            details={"blocked": reason},
        )

    @staticmethod
    def _signal_block_reason(outcome: ExecutionOutcome) -> str:
        """Razón de rechazo para el audit de señales (risk/edge/fatal)."""
        if outcome.error:
            return outcome.error
        if outcome.decision.verdict != RiskVerdict.APPROVED.name:
            return outcome.decision.reason or outcome.decision.verdict
        return ""

    def summary(self) -> TradeSummary:
        stats = self._account.stats(
            self._account.unrealized_pnl(
                self._pm.open_positions(), self._adapter.get_price
            )
        )
        portfolio = self._pm.build_portfolio_state(
            stats.equity,
            initial_equity=self._account.start_equity,
            peak_equity=self._account.peak_equity,
        )
        return TradeSummary(
            equity=stats.equity,
            cash=stats.cash,
            peak_equity=stats.peak_equity,
            drawdown_pct=stats.drawdown_pct,
            realized_pnl=stats.realized_pnl,
            fees_paid=stats.fees_paid,
            unrealized_pnl=stats.unrealized_pnl,
            open_count=portfolio.open_count,
            daily_realized_pnl=portfolio.daily_realized_pnl,
            trades_closed=self._trades_closed,
            consecutive_losses=portfolio.consecutive_losses,
            trading_halted=portfolio.trading_halted,
            safe_mode=portfolio.safe_mode,
            paused=self._paused,
            pause_reason=self._pause_reason,
        )

    def log_summary(self) -> None:
        log.info("paper state", extra=self.summary().to_dict())