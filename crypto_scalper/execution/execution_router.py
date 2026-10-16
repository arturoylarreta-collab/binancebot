"""Execution router (FASE 6) — risk is the gate, execution is the door.

Routes every Signal+FeatureSnapshot through RiskEngine.assess(); only an
APPROVED decision is ever handed to the PositionManager. REJECTED,
SAFE_MODE and TRADING_HALTED decisions are returned without a single order
being sent.

The router also owns the venue selection: for FASE 6 every supported run
mode uses the simulated adapter (full offline). A live adapter plugs in
behind the same interface later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Tuple

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.core.enums import RiskVerdict, SignalType
from crypto_scalper.core.models import (
    ExecutionReport,
    FeatureSnapshot,
    ManagedPosition,
    RiskDecision,
    Signal,
)
from crypto_scalper.execution.base import ExchangeAdapter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.execution.simulated import SimulatedExecutionAdapter
from crypto_scalper.monitoring.metrics import METRICS
from crypto_scalper.risk.cost_model import CostModel, estimate_p_win
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.risk.risk_engine import RiskEngine

from crypto_scalper.execution.filters import FilterRegistry

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionOutcome:
    submitted: bool
    decision: RiskDecision
    position: Optional[ManagedPosition] = None
    error: Optional[str] = None

    @property
    def approved(self) -> bool:
        return self.decision.verdict == RiskVerdict.APPROVED.name


class ExecutionRouter:
    def __init__(
        self,
        risk_engine: RiskEngine,
        position_manager: PositionManager,
        adapter: ExchangeAdapter,
        *,
        tick_size: float = 0.01,
        lot_size: float = 0.0001,
        rr_ratio: float = 2.0,
        sl_atr_mult: float = 1.5,
        cost_model: Optional[CostModel] = None,
        enforce_edge_gate: bool = False,
        filters: Optional[FilterRegistry] = None,
    ) -> None:
        self._risk = risk_engine
        self.filters = filters
        if filters is not None:
            position_manager.filters = filters
        self._pm = position_manager
        self._adapter = adapter
        self._tick_size = tick_size
        self._lot_size = lot_size
        self._rr_ratio = rr_ratio
        self._sl_atr_mult = sl_atr_mult
        self._cost_model = cost_model
        self._enforce_edge_gate = enforce_edge_gate

    @property
    def position_manager(self) -> PositionManager:
        return self._pm

    @property
    def adapter(self) -> ExchangeAdapter:
        return self._adapter

    async def route(
        self,
        signal: Signal,
        snapshot: FeatureSnapshot,
        portfolio: PortfolioState,
    ) -> ExecutionOutcome:
        """Risk first. If approved, open and protect; otherwise stay put."""
        # Only symbols with filters loaded from exchangeInfo are snapped; unit
        # tests and ad-hoc symbols keep the legacy tick/lot defaults.
        sym_filters = (self.filters.get(signal.symbol)
                       if self.filters is not None and signal.symbol in self.filters else None)
        decision = self._risk.assess(
            signal,
            portfolio,
            entry_price=snapshot.price,
            atr=snapshot.atr,
            side="BUY" if signal.signal_type is SignalType.LONG else "SELL",
            tick_size=sym_filters.tick_size if sym_filters else self._tick_size,
            lot_size=sym_filters.step_size if sym_filters else self._lot_size,
            rr_ratio=self._rr_ratio,
            sl_atr_mult=self._sl_atr_mult,
            now_ms=snapshot.timestamp_ms,
        )

        if decision.verdict != RiskVerdict.APPROVED.name:
            METRICS.incr("execution.rejected_by_risk")
            log.info("trade rejected by risk", extra={
                "symbol": signal.symbol,
                "signal_score": signal.score,
                "verdict": decision.verdict,
                "reason": decision.reason,
            })
            return ExecutionOutcome(submitted=False, decision=decision)

        if sym_filters is not None:
            decision, violation = _apply_filters(decision, signal, sym_filters, snapshot.price)
            if violation:
                METRICS.incr("execution.rejected_by_filters")
                log.info("trade below exchange minimums", extra={
                    "symbol": signal.symbol, "detail": violation})
                return ExecutionOutcome(submitted=False, decision=decision,
                                        error=f"exchange_filters: {violation}")

        if self._enforce_edge_gate and self._cost_model is not None:
            if not self._gate_passes(signal, snapshot, decision):
                METRICS.incr("execution.rejected_by_edge")
                log.info("trade blocked by expected-edge gate", extra={
                    "symbol": signal.symbol,
                    "signal_score": signal.score,
                    "decision_verdict": decision.verdict,
                })
                return ExecutionOutcome(
                    submitted=False, decision=decision, error="edge_below_required"
                )

        try:
            position = await self._pm.open(
                signal=signal,
                decision=decision,
                entry_ref_price=snapshot.price,
            )
        except Exception as exc:  # noqa: BLE001
            METRICS.incr("execution.failed")
            log.error("execution failed", extra={
                "symbol": signal.symbol,
                "error": f"{type(exc).__name__}: {exc}",
            })
            return ExecutionOutcome(
                submitted=False, decision=decision,
                error=f"{type(exc).__name__}: {exc}",
            )

        METRICS.incr("execution.submitted")
        return ExecutionOutcome(submitted=True, decision=decision, position=position)

    def _gate_passes(
        self, signal: Signal, snapshot: FeatureSnapshot, decision: RiskDecision
    ) -> bool:
        side = "BUY" if signal.signal_type is SignalType.LONG else "SELL"
        p_win = estimate_p_win(signal, snapshot, side)
        notional = float(decision.notional_value)
        ok, _edge, _cost = self._cost_model.edge_pass(
            p_win=p_win,
            rr_ratio=self._rr_ratio,
            risk_amount=float(decision.risk_amount),
            notional=notional,
            entry_notional=notional,
        )
        return ok

    def update_price(self, symbol: str, price: float) -> Tuple[ExecutionReport, ...]:
        # Only the simulator is price-driven; a live venue matches on its own.
        """Push a market tick into the simulated venue (drives SL/TP/LIMIT)."""
        if hasattr(self._adapter, "set_price"):
            return self._adapter.set_price(symbol, price)
        return ()

    def build_portfolio(
        self,
        equity: float,
        *,
        initial_equity: Optional[float] = None,
        peak_equity: Optional[float] = None,
    ) -> PortfolioState:
        return self._pm.build_portfolio_state(
            equity,
            initial_equity=initial_equity,
            peak_equity=peak_equity,
        )

    async def close(self) -> None:
        await self._pm.close()


def build_execution_stack(
    run_mode: str,
    settings: Settings,
    risk_engine: RiskEngine,
    *,
    venue: Optional[str] = None,
    filters: Optional[FilterRegistry] = None,
    symbols: Optional[list] = None,
    price_source=None,
) -> Tuple[ExchangeAdapter, OrderManager, PositionManager, ExecutionRouter]:
    """Assemble the execution stack.

    ``venue`` = "paper" (simulator, default) or "testnet" (Binance demo futures
    with real orders on a trial account). Real-money LIVE is deliberately not
    buildable from here.
    """
    config = settings.execution
    if run_mode.lower() == "live":
        raise NotImplementedError(
            "real-money live execution is disabled; use EXECUTION_VENUE=testnet"
        )
    filters = filters if filters is not None else FilterRegistry()
    venue = (venue or "paper").lower()

    if venue == "testnet":
        from crypto_scalper.execution.binance_client import BinanceFuturesClient
        from crypto_scalper.execution.binance_futures import BinanceFuturesAdapter

        vc = settings.venue
        client = BinanceFuturesClient(
            vc.testnet_rest_url, vc.api_key, vc.api_secret,
            recv_window_ms=vc.recv_window_ms,
        )
        adapter: ExchangeAdapter = BinanceFuturesAdapter(
            client,
            ws_base_url=vc.testnet_ws_url,
            config=config,
            filters=filters,
            symbols=list(symbols or settings.explicit_symbols or []),
            leverage=settings.risk.max_leverage,
            price_source=price_source,
            poll_interval_s=vc.poll_interval_s,
        )
    else:
        adapter = SimulatedExecutionAdapter(config)
    order_manager = OrderManager(adapter, config)
    position_manager = PositionManager(order_manager, config)
    router = ExecutionRouter(
        risk_engine,
        position_manager,
        adapter,
        rr_ratio=config.default_rr_ratio,
        sl_atr_mult=config.default_sl_atr_mult,
        filters=filters,   # shared: the testnet adapter fills it on start()
    )
    return adapter, order_manager, position_manager, router


def _apply_filters(decision: RiskDecision, signal: Signal, f, entry_price: float):
    """Snap an APPROVED decision to the symbol grid and enforce minimums.

    SL rounds AWAY from entry (risk distance never shrinks), TP rounds TOWARD
    entry (stays reachable); quantity floors to the lot step.
    """
    import dataclasses

    long = signal.signal_type is SignalType.LONG
    qty = f.floor_qty(float(decision.position_size or 0.0))
    sl = f.round_price(float(decision.stop_loss_price), "down" if long else "up")
    tp = f.round_price(float(decision.take_profit_price), "down" if long else "up")
    violation = f.check(qty, entry_price)
    if not violation and (sl <= 0 or tp <= 0 or (long and not sl < entry_price < tp)
                          or (not long and not tp < entry_price < sl)):
        violation = f"protection off-grid after rounding (sl={sl}, tp={tp})"
    decision = dataclasses.replace(
        decision,
        position_size=qty,
        stop_loss_price=sl,
        take_profit_price=tp,
        notional_value=qty * entry_price,
        risk_amount=qty * abs(entry_price - sl),
    )
    return decision, violation
