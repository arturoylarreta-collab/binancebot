"""Persistence-gateway observer — read-only with respect to the engines.

`ObservabilityRepository` envuelve un `Repository` (SQLite normalmente) y se
inyecta donde el orquestador espera el repo (main.py / motores). Su trabajo:

  * derivar las alertas TRADE_OPENED / TRADE_CLOSED a partir de las
    transiciones de estado de posición que ya persisten (sin tocar la venue);
  * derivar RISK_HALT / KILL_SWITCH desde las decisiones de riesgo que ya
    persisten (con cooldown para no spamear mientras el halt persiste);
  * calcular `pnl_unrealized` por posición usando un mark provider (el precio
    del adapter/venue, se adjunta por duck-typing desde los engines);
  * escribir un heartbeat periódico (equity, drawdown, exposición, trades del
    día) que alimenta al dashboard de observabilidad.

Garantías:
  * NUNCA bloquea el hot path por alertas: los mensajes van a una cola
    acotada (`emit` no hace await).
  * Las escrituras extra son las mismas que el orquestador ya hace por ciclo
    de persistencia (misma clase de coste, misma frecuencia).
  * No importa ni modifica Risk Engine / Execution Engine.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from crypto_scalper.core.enums import PositionStatus
from crypto_scalper.core.models import (
    ExecutionReport,
    FeatureSnapshot,
    Fill,
    HeartbeatSnapshot,
    ManagedPosition,
    RiskDecision,
    Signal,
)
from crypto_scalper.execution.reconciliation import ReconciliationReport
from crypto_scalper.monitoring.alerts import AlertEvent, TelegramNotifier
from crypto_scalper.storage.base import Repository

log = logging.getLogger(__name__)

_OPEN_STATUSES = frozenset({
    PositionStatus.ENTRY_SUBMITTED.name,
    PositionStatus.ENTRY_FILLED.name,
    PositionStatus.PROTECTING.name,
    PositionStatus.ACTIVE.name,
})
_TERMINAL_STATUSES = frozenset({PositionStatus.CLOSED.name, PositionStatus.ABORTED.name})
_DAY_MS = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


class ObservabilityRepository(Repository):
    """Envuelve un Repository y añade observación no-bloqueante.

    mark_price: optional `(symbol) -> float` para PnL no realizado.
    notifier: optional TelegramNotifier (ya empezado por el caller).
    """

    def __init__(
        self,
        repository: Repository,
        *,
        notifier: Optional[TelegramNotifier] = None,
        mode: str = "paper",
        start_equity: float = 10_000.0,
        heartbeat_interval_ms: int = 5_000,
        halt_cooldown_ms: int = 300_000,
    ) -> None:
        self._inner = repository
        self._notifier = notifier
        self._mode = mode
        self._start_equity = float(start_equity)
        self._heartbeat_interval_ms = int(heartbeat_interval_ms)
        self._halt_cooldown_ms = int(halt_cooldown_ms)

        self._mark: Optional[Callable[[str], float]] = None
        self._marks: Dict[str, float] = {}
        self._started_ts_ms = _now_ms()
        self._peak_equity = float(start_equity)

        self._position_status: Dict[str, str] = {}
        self._halt_last_ts: Dict[str, int] = {}
        self._last_heartbeat_ts_ms: int = 0

    # ── wiring (duck-typed desde los engines, sin imports cruzados) ─────────

    def attach_mark(self, fn: Callable[[str], float]) -> None:
        self._mark = fn

    def set_mode(self, mode: str) -> None:
        self._mode = mode

    @property
    def inner(self) -> Repository:
        return self._inner

    # ── lifecycle passthrough ────────────────────────────────────────────────

    async def close(self) -> None:
        await self._inner.close()

    # ── generic pipeline persistence ─────────────────────────────────────────

    async def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        self._marks[snapshot.symbol] = float(snapshot.price)
        await self._inner.save_feature_snapshot(snapshot)

    async def save_event(self, table: str, payload: dict) -> None:
        await self._inner.save_event(table, payload)

    # ── trading audit trail ─────────────────────────────────────────────────

    async def save_position(
        self, position: ManagedPosition, pnl_unrealized: Optional[float] = None
    ) -> None:
        status = position.status.name
        previous = self._position_status.get(position.position_id)
        self._position_status[position.position_id] = status

        if pnl_unrealized is None:
            pnl_unrealized = self._unrealized(position)

        await self._inner.save_position(position, pnl_unrealized=pnl_unrealized)

        if status in _TERMINAL_STATUSES and previous in _OPEN_STATUSES:
            self._notify_trade_closed(position)
        elif status in _OPEN_STATUSES and previous not in _OPEN_STATUSES:
            self._notify_trade_opened(position)

        await self._maybe_heartbeat()

    async def save_order(self, report: ExecutionReport) -> None:
        await self._inner.save_order(report)

    async def save_fills(self, fills: Tuple[Fill, ...]) -> None:
        await self._inner.save_fills(fills)

    async def save_reconciliation(self, report: ReconciliationReport) -> None:
        await self._inner.save_reconciliation(report)

    async def save_risk_decision(self, decision: RiskDecision) -> None:
        self._notify_halt(decision)
        await self._inner.save_risk_decision(decision)
        await self._maybe_heartbeat()

    async def save_signal(self, signal: Signal, rejection_reason: str = "") -> None:
        await self._inner.save_signal(signal, rejection_reason=rejection_reason)

    async def save_heartbeat(self, snapshot: HeartbeatSnapshot) -> None:
        await self._inner.save_heartbeat(snapshot)

    async def load_positions(self) -> List[ManagedPosition]:
        return await self._inner.load_positions()

    async def load_orders(self) -> List[ExecutionReport]:
        return await self._inner.load_orders()

    # ── observation helpers ─────────────────────────────────────────────────

    def _mark_for(self, symbol: str) -> Optional[float]:
        try:
            if self._mark is not None:
                value = self._mark(symbol)
                if value is not None and value > 0:
                    return float(value)
        except Exception:  # noqa: BLE001 - un mark roto nunca rompe el repo
            pass
        return self._marks.get(symbol)

    def _unrealized(self, position: ManagedPosition) -> Optional[float]:
        if position.status.name not in _OPEN_STATUSES:
            return 0.0
        mark = self._mark_for(position.symbol)
        if mark is None:
            return None
        sign = 1.0 if position.side.upper() == "BUY" else -1.0
        return round((mark - position.entry_price) * position.quantity * sign, 6)

    def _notify_trade_opened(self, position: ManagedPosition) -> None:
        if self._notifier is None:
            return
        self._notifier.notify_trade_opened(
            symbol=position.symbol,
            entry_price=position.entry_price,
            quantity=position.quantity,
            stop_loss=position.stop_loss_price,
            take_profit=position.take_profit_price,
        )

    def _notify_trade_closed(self, position: ManagedPosition) -> None:
        if self._notifier is None:
            return
        notional = max(1e-12, position.entry_price * position.quantity)
        pnl_pct = 100.0 * position.realized_pnl / notional
        self._notifier.notify_trade_closed(
            symbol=position.symbol,
            pnl_pct=pnl_pct,
            pnl_usd=position.realized_pnl,
            reason=position.close_reason,
        )

    def _notify_halt(self, decision: RiskDecision) -> None:
        alert = _halt_alert_from_decision(decision)
        if alert is None:
            return
        event, human_reason = alert
        if self._notifier is None:
            return
        key = f"{event.name}:{decision.reason}:{decision.symbol}"
        now = _now_ms()
        if now - self._halt_last_ts.get(key, 0) < self._halt_cooldown_ms:
            return
        self._halt_last_ts[key] = now
        if event is AlertEvent.KILL_SWITCH:
            self._notifier.notify_kill_switch(human_reason)
        else:
            self._notifier.notify_risk_halt(human_reason)

    async def _maybe_heartbeat(self) -> None:
        now = _now_ms()
        if now - self._last_heartbeat_ts_ms < self._heartbeat_interval_ms:
            return
        self._last_heartbeat_ts_ms = now
        await self._inner.save_heartbeat(await self._build_heartbeat(now))

    async def _build_heartbeat(self, now_ms: int) -> HeartbeatSnapshot:
        realized_total = 0.0
        trades_today = 0
        open_positions: List[ManagedPosition] = []
        day_start = now_ms - (now_ms % _DAY_MS)
        try:
            for p in await self._inner.load_positions():
                if p.status.name == PositionStatus.CLOSED.name:
                    realized_total += p.realized_pnl
                    if p.closed_ts_ms >= day_start:
                        trades_today += 1
                elif p.status.name in _OPEN_STATUSES:
                    open_positions.append(p)
        except Exception:  # noqa: BLE001 - heartbeat nunca debe romper el loop
            log.warning("heartbeat could not load positions", exc_info=True)

        unrealized_total = 0.0
        exposure = 0.0
        for p in open_positions:
            unreal = self._unrealized(p)
            unrealized_total += unreal if unreal is not None else 0.0
            exposure += float(p.notional_value or 0.0)

        equity = self._start_equity + realized_total + unrealized_total
        self._peak_equity = max(self._peak_equity, equity)
        dd = 0.0
        if self._peak_equity > 0:
            dd = max(0.0, (self._peak_equity - equity) / self._peak_equity)

        return HeartbeatSnapshot(
            ts_ms=now_ms,
            mode=self._mode,
            status="running",
            equity=round(equity, 6),
            realized_pnl=round(realized_total, 6),
            unrealized_pnl=round(unrealized_total, 6),
            drawdown_pct=round(dd, 8),
            total_exposure=round(exposure, 6),
            open_count=len(open_positions),
            trades_today=trades_today,
            uptime_ms=max(0, now_ms - self._started_ts_ms),
        )


def _halt_alert_from_decision(decision: RiskDecision) -> Optional[Tuple[AlertEvent, str]]:
    """Verdict/reason del Risk Engine → evento de alerta (con cooldown externo)."""
    reason = decision.reason or ""
    if reason == "DAILY_LOSS_LIMIT":
        return (AlertEvent.RISK_HALT, "limite diario de perdidas alcanzado")
    if reason == "MAX_DRAWDOWN":
        return (AlertEvent.RISK_HALT, "max drawdown alcanzado")
    if reason == "KILL_SWITCH" or decision.verdict == "SAFE_MODE":
        return (AlertEvent.KILL_SWITCH, "kill switch activado por proteccion")
    return None