"""Backtest report (FASE 8) — deterministic summary from closed positions.

Builds a `BacktestReport` from the same `ManagedPosition` objects the live
engine produces: no extra bookkeeping. Win rate, profit factor, Sharpe (por
barras), max drawdown y por-bloque edge attribution se calculan aquí en
offline, sin rumores.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from crypto_scalper.config.backtest import CostModelConfig
from crypto_scalper.core.models import ManagedPosition
from crypto_scalper.risk.cost_model import CostModel


@dataclass(frozen=True)
class TradeRecord:
    position_id: str
    symbol: str
    side: str
    quantity: float
    entry_price: float
    exit_price: float
    realized_pnl: float
    open_ts_ms: int
    close_ts_ms: int
    close_reason: str
    regime: str

    @classmethod
    def from_managed(cls, p: ManagedPosition, exit_price: float = 0.0) -> "TradeRecord":
        return cls(
            position_id=p.position_id,
            symbol=p.symbol,
            side=p.side,
            quantity=p.quantity,
            entry_price=p.entry_price,
            exit_price=exit_price if exit_price else p.entry_price + (
                p.realized_pnl / (p.quantity * (1.0 if p.side == "BUY" else -1.0))
                if p.quantity > 0 else 0.0
            ),
            realized_pnl=p.realized_pnl,
            open_ts_ms=p.opened_ts_ms,
            close_ts_ms=p.closed_ts_ms,
            close_reason=p.close_reason,
            regime=p.regime or "",
        )


@dataclass
class EquityPoint:
    ts_ms: int
    equity: float
    drawdown_pct: float = 0.0


@dataclass
class EdgeBlock:
    label: str
    count: int
    wins: int = 0
    losses: int = 0
    entry_notional: float = 0.0
    exit_notional: float = 0.0
    gross_pnl: float = 0.0
    attributed_cost: float = 0.0
    net_pnl: float = 0.0


class BacktestReport:
    def __init__(
        self,
        trades: List[ManagedPosition],
        portfolio_marks: List[Tuple[int, float]],
        initial_equity: float,
        cost_model_config: CostModelConfig,
        edge_blocked: int = 0,
        settings_summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._initial_equity = initial_equity
        self._edge_blocked = edge_blocked
        self._settings_summary = settings_summary or {}

        self._records = [TradeRecord.from_managed(p) for p in trades]
        self._cost_model = CostModel(cost_model_config)
        self._equity_curve = self._build_equity_curve(portfolio_marks)
        self._stats = self._compute_stats()
        self._by_symbol = self._group_by_symbol()
        self._by_reason = self._group_by_reason()
        self._edge_blocks = self._compute_edge_blocks()

    # ── Query interface ──────────────────────────────────────────────────────

    @property
    def stats(self) -> Dict[str, Any]:
        return dict(self._stats)

    @property
    def records(self) -> List[TradeRecord]:
        return list(self._records)

    @property
    def equity_curve(self) -> List[EquityPoint]:
        return list(self._equity_curve)

    @property
    def by_symbol(self) -> Dict[str, Any]:
        return dict(self._by_symbol)

    @property
    def by_reason(self) -> Dict[str, Any]:
        return dict(self._by_reason)

    @property
    def edge_blocks(self) -> List[EdgeBlock]:
        return list(self._edge_blocks)

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stats": self._stats,
            "equity_curve_points": len(self._equity_curve),
            "trades": len(self._records),
            "by_symbol": self._by_symbol,
            "by_reason": self._by_reason,
            "edge_blocks": [b.__dict__ for b in self._edge_blocks],
            "settings": self._settings_summary,
        }

    def summary_text(self) -> str:
        s = self._stats
        lines = [
            f"Backtest — {s['trades']} trades | {s['win_rate']:.1%} win | "
            f"PF {s['profit_factor']:.2f} | Sharpe {s['sharpe_per_bar']:.2f} | "
            f"MaxDD {s['max_drawdown_pct']:.1%}",
            f"Equity: {self._initial_equity:.0f} → {s['final_equity']:.0f} "
            f"(net {s['total_net_pnl']:+.2f})",
        ]
        if self._edge_blocked:
            lines.append(f"Edge blocked: {self._edge_blocked}")
        if self._by_symbol:
            lines.append("By-symbol: " + ", ".join(
                f"{sym}: {b['trades']}t {b['net_pnl']:+.2f}"
                for sym, b in self._by_symbol.items()
            ))
        if self._by_reason:
            lines.append("By-reason: " + ", ".join(
                f"{r}: {b['trades']}t {b['net_pnl']:+.2f}"
                for r, b in self._by_reason.items()
            ))
        return "\n".join(lines)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _build_equity_curve(
        self, marks: List[Tuple[int, float]]
    ) -> List[EquityPoint]:
        if not marks:
            return [EquityPoint(ts_ms=0, equity=self._initial_equity)]
        points: List[EquityPoint] = []
        peak = self._initial_equity
        for ts_ms, eq in marks:
            peak = max(peak, eq)
            dd = (peak - eq) / peak if peak > 0 else 0.0
            points.append(EquityPoint(ts_ms=ts_ms, equity=eq, drawdown_pct=dd))
        return points

    def _compute_stats(self) -> Dict[str, Any]:
        records = self._records
        n = len(records)
        wins = [r for r in records if r.realized_pnl > 0]
        losses = [r for r in records if r.realized_pnl <= 0]
        gross_win = sum(r.realized_pnl for r in wins) if wins else 0.0
        gross_loss = abs(sum(r.realized_pnl for r in losses)) if losses else 0.0
        total_net = sum(r.realized_pnl for r in records)
        final = self._equity_curve[-1].equity if self._equity_curve else self._initial_equity
        peak = max((p.equity for p in self._equity_curve), default=self._initial_equity)
        max_dd = max((p.drawdown_pct for p in self._equity_curve), default=0.0)
        avg_pnl = total_net / n if n else 0.0
        variance = sum((r.realized_pnl - avg_pnl) ** 2 for r in records) / max(1, n)
        stddev = math.sqrt(variance)
        sharpe = (avg_pnl / stddev) if stddev > 1e-12 else 0.0
        pf = (gross_win / gross_loss) if gross_loss > 1e-12 else (float("inf") if gross_win > 0 else 0.0)
        return {
            "trades": n,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / n if n else 0.0,
            "profit_factor": pf,
            "sharpe_per_bar": sharpe,
            "avg_pnl": avg_pnl,
            "max_drawdown_pct": max_dd,
            "peak_equity": peak,
            "final_equity": final,
            "total_net_pnl": total_net,
            "initial_equity": self._initial_equity,
            "edge_blocked": self._edge_blocked,
        }

    def _group_by_symbol(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Any] = {}
        for r in self._records:
            bucket = out.setdefault(r.symbol, {"trades": 0, "net_pnl": 0.0, "wins": 0})
            bucket["trades"] += 1
            bucket["net_pnl"] += r.realized_pnl
            if r.realized_pnl > 0:
                bucket["wins"] += 1
        for b in out.values():
            b["win_rate"] = b["wins"] / b["trades"] if b["trades"] else 0.0
            b["net_pnl"] = round(b["net_pnl"], 4)
        return out

    def _group_by_reason(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Any] = {}
        for r in self._records:
            reason = r.close_reason or "unknown"
            bucket = out.setdefault(reason, {"trades": 0, "net_pnl": 0.0, "wins": 0})
            bucket["trades"] += 1
            bucket["net_pnl"] += r.realized_pnl
            if r.realized_pnl > 0:
                bucket["wins"] += 1
        for b in out.values():
            b["win_rate"] = b["wins"] / b["trades"] if b["trades"] else 0.0
            b["net_pnl"] = round(b["net_pnl"], 4)
        return out

    def _compute_edge_blocks(self) -> List[EdgeBlock]:
        """Agrupa trades por reason (stop_loss/take_profit) y atribuye costes.

        La atribución usa los notional reales entry/exit de cada trade y las
        tasas del CostModelConfig (fees+spread+latencia+funding), con holding
        desconocido modelado como 0h (funding ≈ 0). Es una cota superior del
        net: el slippage ya está dentro del gross.
        """
        blocks: Dict[str, EdgeBlock] = {}
        for r in self._records:
            label = r.close_reason or "unknown"
            b = blocks.setdefault(label, EdgeBlock(label=label, count=0))
            b.count += 1
            b.gross_pnl += r.realized_pnl
            b.wins += 1 if r.realized_pnl > 0 else 0
            b.losses += 1 if r.realized_pnl <= 0 else 0
            b.entry_notional += r.quantity * r.entry_price
            b.exit_notional += r.quantity * r.exit_price
        for b in blocks.values():
            breakdown = self._cost_model.round_trip(
                entry_notional=b.entry_notional, exit_notional=b.exit_notional
            )
            b.attributed_cost = breakdown.attributed_total
            b.net_pnl = round(b.gross_pnl - breakdown.attributed_total, 4)
        return sorted(blocks.values(), key=lambda b: b.label)