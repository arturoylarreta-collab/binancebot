"""Cost model and expected-edge gate (FASE 8).

Batí la arquitectura FASE 5: el Risk Engine ya aprobaba decisiones con el
gate `minimum_edge` abierto (`checks["minimum_edge"] = True`). FASE 8
materializa ese gate: un modelo de costes explícito y comprobable al
`ExecutionRouter`/`BacktestEngine`.

Modelo de costes honesto (sin doble contabilidad):

  * `slippage_pct` ya está EMBEBIDO en los fill prices del venue de backtest
    (impacto de mercado + retraso de ejecución), por lo que el gross PnL ya
    lo refleja; no se vuelve a atribuir como coste añadido, pero sí entra en
    el gate de expected edge.
  * `fees + spread + funding + latency` se atribuyen sobre el notional medio
    del roundtrip para que el reporte muestre el PnL neto tras costes.

`expected_edge_pct` convierte la probabilidad de acierto y el reward-risk
en el edge esperado como % del notional usando la cantidad arriesgada (R):
   E[R] = P(win) * RR - (1 - P(win))
Luego:  edge_after_costs = edge_pct - total_cost_pct.

Cuando `enforce_edge_gate` está activo, una señal cuya expectativa después de
costes quede por debajo de `minimum_required_edge_pct` NO llega a ejecutarse
(el gate vive en ExecutionRouter, igual que en el camino live).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from crypto_scalper.config.backtest import CostModelConfig
from crypto_scalper.core.models import FeatureSnapshot, Signal

_ML_KEYS = ("p_up", "p_down")


@dataclass(frozen=True)
class CostBreakdown:
    """Per-trade cost attribution. Pure numbers, no exchange logic."""

    entry_notional: float
    exit_notional: float
    holding_hours: float
    fee_pct: float
    spread_pct: float
    slippage_pct: float          # informational: ya embebido en los fills
    latency_pct: float
    funding_pct_per_8h: float

    @property
    def avg_notional(self) -> float:
        return max(1e-12, (self.entry_notional + self.exit_notional) / 2.0)

    @property
    def fees(self) -> float:
        return (self.entry_notional + self.exit_notional) * self.fee_pct

    @property
    def spread(self) -> float:
        return self.avg_notional * 2.0 * self.spread_pct

    @property
    def latency(self) -> float:
        return self.avg_notional * 2.0 * self.latency_pct

    @property
    def funding(self) -> float:
        return self.avg_notional * self.funding_pct_per_8h * (self.holding_hours / 8.0)

    @property
    def slippage_attributed(self) -> float:
        """0.0 de atribución: el slippage ya está en los fill prices."""
        return 0.0

    @property
    def attributed_total(self) -> float:
        return self.fees + self.spread + self.latency + self.funding

    @property
    def attributed_total_pct(self) -> float:
        return self.attributed_total / self.avg_notional

    def to_dict(self) -> dict:
        return {
            "entry_notional": round(self.entry_notional, 4),
            "exit_notional": round(self.exit_notional, 4),
            "holding_hours": round(self.holding_hours, 4),
            "fees": round(self.fees, 4),
            "spread": round(self.spread, 4),
            "slippage_attributed": self.slippage_attributed,
            "latency": round(self.latency, 4),
            "funding": round(self.funding, 4),
            "attributed_total": round(self.attributed_total, 4),
            "attributed_total_pct": round(self.attributed_total_pct, 8),
        }


class CostModel:
    def __init__(self, config: CostModelConfig) -> None:
        self._config = config

    @property
    def config(self) -> CostModelConfig:
        return self._config

    def round_trip(
        self,
        *,
        entry_notional: float,
        exit_notional: Optional[float] = None,
        holding_hours: float = 0.0,
    ) -> CostBreakdown:
        exit_notional = float(exit_notional if exit_notional is not None else entry_notional)
        return CostBreakdown(
            entry_notional=float(entry_notional),
            exit_notional=exit_notional,
            holding_hours=float(holding_hours),
            fee_pct=self._config.fee_pct,
            spread_pct=self._config.spread_pct,
            slippage_pct=self._config.slippage_pct,
            latency_pct=self._config.latency_pct,
            funding_pct_per_8h=self._config.funding_pct_per_8h,
        )

    def total_cost_pct(
        self,
        *,
        entry_notional: float,
        exit_notional: Optional[float] = None,
        holding_hours: float = 0.0,
    ) -> float:
        """Porcentaje del notional consumido por TODOS los costes del roundtrip."""
        b = self.round_trip(entry_notional=entry_notional,
                            exit_notional=exit_notional,
                            holding_hours=holding_hours)
        return b.attributed_total_pct + 2.0 * self._config.slippage_pct

    def expected_edge_pct(
        self,
        *,
        p_win: float,
        rr_ratio: float,
        risk_amount: float,
        notional: float,
    ) -> float:
        """Edge esperado como % del notional dadas P(win) y el reward-risk."""
        r_multiple = p_win * rr_ratio - (1.0 - p_win)
        return r_multiple * max(0.0, risk_amount) / max(1e-12, notional)

    def edge_pass(
        self,
        *,
        p_win: float,
        rr_ratio: float,
        risk_amount: float,
        notional: float,
        entry_notional: float,
        exit_notional: Optional[float] = None,
        holding_hours: float = 0.0,
    ) -> tuple:
        """(aprobado, edge_pct, total_cost_pct) del gate de expected edge."""
        edge = self.expected_edge_pct(
            p_win=p_win, rr_ratio=rr_ratio, risk_amount=risk_amount, notional=notional
        )
        cost = self.total_cost_pct(
            entry_notional=entry_notional,
            exit_notional=exit_notional,
            holding_hours=holding_hours,
        )
        return (edge - cost >= self._config.minimum_required_edge_pct), edge, cost


def p_win_from_score(score: float, side: str) -> float:
    """Estimación de P(win) sin ML a partir del Signal Score.

    Mapea [0, 100] → [0.25, 0.75] de probabilidad de que el movimiento
    esperado ocurra en la dirección de la señal (alta señal long ⇒ alta
    P(win) si la posición es LONG). Es una hipótesis no validada: se usa
    solo como fallback cuando no hay predictor ML cableado.
    """
    z = (float(score) - 50.0) / 50.0
    p = 0.5 + 0.25 * z
    if str(side).upper() == "SELL":
        p = 1.0 - p
    return max(0.05, min(0.95, p))


def estimate_p_win(signal: Signal, snapshot: FeatureSnapshot, side: str) -> float:
    """P(win) del trade: P(up)/P(down) del ML si existe; si no, score→prob."""
    ml = (snapshot.features_by_category or {}).get("ml", {})
    key = "p_up" if str(side).upper() == "BUY" else "p_down"
    p = ml.get(key)
    if isinstance(p, (int, float)) and 0.0 <= float(p) <= 1.0:
        return float(p)
    return p_win_from_score(signal.score, side)