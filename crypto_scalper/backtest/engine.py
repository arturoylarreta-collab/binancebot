"""Backtest engine (FASE 8) — deterministic offline replay using the real stack.

Modo de operación: BarReplay → FeatureEngine → SignalEngine → [ML] → Risk →
Orchestrator(BarVenue). La venue adjunta es la `BarVenue` (offline, bar-driven).
El MemoryBank(MusicBox), NewsStore y RiskEngine se construyen vacíos por
defecto; `cost_model` se cablea al `ExecutionRouter` para el gate de edge.

Fuentes de verdad dentro del engine:

  * `BarVenue.process_bar` evalúa protecciones (SL/TP/LIMIT) intrabar.
  * `TradeOrchestrator.on_price` asegura el mark-to-market al close de cada
    barra.
  * `FeatureEngine.compute` lee `SymbolState` (candles 1s sintéticos +
    orderbook sintético + trades sintéticos) exactamente como en live.
  * `SignalEngine.evaluate` usa los pesos reales de FASE 3.

No hay ni UNA línea de fill o risk weight hardcodeada aquí: la cadena real
es la autoridad.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from crypto_scalper.backtest.data import KlineCorpus, kline_to_trades, synthetic_orderbook
from crypto_scalper.backtest.reports import BacktestReport
from crypto_scalper.backtest.venue import BarVenue
from crypto_scalper.config.paper import PaperConfig
from crypto_scalper.config.settings import Settings
from crypto_scalper.core.enums import SignalType
from crypto_scalper.core.models import FeatureSnapshot
from crypto_scalper.execution.execution_router import ExecutionRouter
from crypto_scalper.execution.order_manager import OrderManager
from crypto_scalper.execution.position_manager import PositionManager
from crypto_scalper.features.feature_engine import FeatureEngine
from crypto_scalper.market_data.state import SymbolState
from crypto_scalper.risk.cost_model import CostModel
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.base import NoopRepository
from crypto_scalper.trading.account import PaperAccount
from crypto_scalper.trading.orchestrator import TradeOrchestrator

log = logging.getLogger(__name__)


class BacktestEngine:
    """Motor de backtesting offline FASE 8."""

    def __init__(
        self,
        *,
        settings: Settings,
        corpus: KlineCorpus,
        feature_engine: FeatureEngine,
        signal_engine: Any,
        predictor: Any = None,
        news_store: Any = None,
        repository: Any = None,
    ) -> None:
        self._settings = settings
        self._config = settings.backtest
        self._corpus = corpus
        self._feature_engine = feature_engine
        self._signal_engine = signal_engine
        self._predictor = predictor

        self._cost_model = CostModel(self._config.costs)
        self._portfolio_marks: List[tuple] = []   # (ts_ms, equity)
        self._pending_next_open: Dict[str, tuple] = {}  # symbol → (signal, snapshot)
        self._edge_blocked: int = 0
        self._entries_opened: int = 0

        risk_engine = RiskEngine(settings.risk)
        venue = BarVenue(
            settings.execution,
            entry_fill_fraction=self._config.entry_fill_fraction,
        )
        order_manager = OrderManager(venue, settings.execution)
        position_manager = PositionManager(order_manager, settings.execution)
        router = ExecutionRouter(
            risk_engine,
            position_manager,
            venue,
            rr_ratio=settings.execution.default_rr_ratio,
            sl_atr_mult=settings.execution.default_sl_atr_mult,
            cost_model=self._cost_model,
            enforce_edge_gate=self._config.enforce_edge_gate,
        )
        account = PaperAccount(self._config.start_equity, fee_pct=0.0)
        repository = repository or NoopRepository()
        orchestrator = TradeOrchestrator(
            router,
            signal_engine,
            account,
            repository,
            order_manager=order_manager,
            reconciler=None,
            config=PaperConfig(max_open_per_symbol=self._config.max_open_per_symbol),
        )

        attach_mark = getattr(repository, "attach_mark", None)
        if attach_mark is not None:
            attach_mark(venue.get_price)
        set_mode = getattr(repository, "set_mode", None)
        if set_mode is not None:
            set_mode("backtest")

        self._venue = venue
        self._orchestrator = orchestrator

    async def run(self, max_trades: int = 0) -> BacktestReport:
        """Reproduce el corpus y devuelve el reporte. max_trades=0 ∞."""
        self._pending_next_open.clear()
        self._portfolio_marks.clear()
        self._edge_blocked = 0
        self._entries_opened = 0

        states: Dict[str, SymbolState] = {
            s: SymbolState(s) for s in self._corpus.symbols
        }

        await self._orchestrator.start()

        for event in self._corpus.events():
            if max_trades and self._entries_opened >= max_trades:
                break
            await self._process_event(event, states)

        await self._orchestrator.close()

        trades = list(self._orchestrator.closed_positions())

        return BacktestReport(
            trades=trades,
            portfolio_marks=self._portfolio_marks,
            initial_equity=self._config.start_equity,
            cost_model_config=self._config.costs,
            edge_blocked=self._edge_blocked,
            settings_summary=self._settings_summary(),
        )

    async def _process_event(self, event: Any, states: Dict[str, SymbolState]) -> None:
        symbol, candle = event.symbol, event.candle
        state = states[symbol]
        bar_close_ts = candle.ts_ms + candle.interval_s * 1000 - 1

        self._venue.set_clock(bar_close_ts)

        for t in kline_to_trades(symbol, candle, steps=min(30, candle.interval_s)):
            state.on_trade(t)
        bids, asks = synthetic_orderbook(candle)
        state.orderbook.apply_snapshot(
            last_update_id=candle.ts_ms,
            bids=bids,
            asks=asks,
            ts_ms=bar_close_ts,
        )

        if self._config.fill_at == "next_open":
            pending = self._pending_next_open.pop(symbol, None)
            if pending is not None:
                self._venue.set_clock(candle.ts_ms)
                self._venue.set_price(symbol, candle.open)
                signal, snapshot = pending
                await self._maybe_open(signal, snapshot)
                await asyncio.sleep(0)

        self._venue.process_bar(symbol, candle)
        await asyncio.sleep(0)

        await self._orchestrator.on_price(symbol, candle.close)
        await asyncio.sleep(0)

        if state.candles.count >= self._config.warmup_bars:
            snapshot = self._feature_engine.compute(state, now_ms=bar_close_ts)
            signal = self._signal_engine.evaluate(snapshot)
            if signal.signal_type in (SignalType.LONG, SignalType.SHORT) and signal.eligible:
                if self._config.fill_at == "close":
                    await self._maybe_open(signal, snapshot)
                else:
                    self._pending_next_open[symbol] = (signal, snapshot)

        await self._orchestrator.persist()
        self._portfolio_marks.append((bar_close_ts, self._orchestrator.build_portfolio().equity))

    async def _maybe_open(self, signal: Any, snapshot: FeatureSnapshot) -> None:
        outcome = await self._orchestrator.on_signal(signal, snapshot)
        if outcome.error == "edge_below_required":
            self._edge_blocked += 1
        if outcome.submitted:
            self._entries_opened += 1
        if not outcome.submitted:
            log.info("backtest route blocked", extra={
                "symbol": signal.symbol,
                "score": signal.score,
                "error": outcome.error,
                "verdict": outcome.decision.verdict,
            })

    def _settings_summary(self) -> Dict[str, Any]:
        return {
            "interval_s": self._config.interval_s,
            "warmup_bars": self._config.warmup_bars,
            "fill_at": self._config.fill_at,
            "start_equity": self._config.start_equity,
            "enforce_edge_gate": self._config.enforce_edge_gate,
            "edge_blocked": self._edge_blocked,
            "costs": self._cost_model.config.__dict__,
        }