"""PaperAccount (FASE 7) — simulated balance for the offline venue.

Tracks cash, realized PnL and fees as positions close, plus the peak equity
needed for drawdown. Unrealized PnL is computed on demand from the open
positions and the market's last price, so `equity()` is a realistic
mark-to-market balance.

Fees are a configurable placeholder (taker fee applied to entry + exit
notional); the simulated adapter itself charges nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from crypto_scalper.core.models import ManagedPosition


@dataclass(frozen=True)
class PaperAccountStats:
    start_equity: float
    cash: float
    realized_pnl: float
    fees_paid: float
    unrealized_pnl: float
    equity: float
    peak_equity: float

    def to_dict(self) -> dict:
        return {
            "start_equity": round(self.start_equity, 6),
            "cash": round(self.cash, 6),
            "realized_pnl": round(self.realized_pnl, 6),
            "fees_paid": round(self.fees_paid, 6),
            "unrealized_pnl": round(self.unrealized_pnl, 6),
            "equity": round(self.equity, 6),
            "peak_equity": round(self.peak_equity, 6),
            "drawdown_pct": round(self.drawdown_pct, 6),
        }

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - self.equity) / self.peak_equity)


class PaperAccount:
    def __init__(self, start_equity: float, fee_pct: float = 0.0) -> None:
        if start_equity <= 0:
            raise ValueError("start_equity must be > 0")
        if not (0.0 <= fee_pct < 0.10):
            raise ValueError("fee_pct must be in [0, 0.10)")
        self._start_equity = float(start_equity)
        self._fee_pct = float(fee_pct)
        self._cash = float(start_equity)
        self._realized_pnl = 0.0
        self._fees_paid = 0.0
        self._peak_equity = float(start_equity)

    @property
    def start_equity(self) -> float:
        return self._start_equity

    @property
    def fee_pct(self) -> float:
        return self._fee_pct

    @property
    def realized_pnl(self) -> float:
        return self._realized_pnl

    @property
    def fees_paid(self) -> float:
        return self._fees_paid

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def peak_equity(self) -> float:
        return self._peak_equity

    # ── accounting ───────────────────────────────────────────────────────────

    def realize_close(self, position: ManagedPosition) -> None:
        """Settle one closed position: add its realized PnL and charge fees."""
        qty = float(position.quantity)
        if qty <= 0:
            return
        sign = 1.0 if position.side == "BUY" else -1.0
        exit_price = position.entry_price + position.realized_pnl / (sign * qty)
        entry_notional = position.notional_value or position.entry_price * qty
        exit_notional = max(0.0, exit_price) * qty
        fee = (entry_notional + exit_notional) * self._fee_pct
        self._realized_pnl += float(position.realized_pnl)
        self._fees_paid += fee
        self._cash += float(position.realized_pnl) - fee

    def unrealized_pnl(
        self,
        positions: Iterable[ManagedPosition],
        get_price: Callable[[str], float],
    ) -> float:
        total = 0.0
        for pos in positions:
            price = get_price(pos.symbol)
            if not price or price <= 0:
                continue
            sign = 1.0 if pos.side == "BUY" else -1.0
            total += (price - pos.entry_price) * sign * pos.quantity
        return total

    def stats(
        self,
        unrealized_pnl: float = 0.0,
    ) -> PaperAccountStats:
        equity = self._cash + unrealized_pnl
        if equity > self._peak_equity:
            self._peak_equity = equity
        return PaperAccountStats(
            start_equity=self._start_equity,
            cash=self._cash,
            realized_pnl=self._realized_pnl,
            fees_paid=self._fees_paid,
            unrealized_pnl=unrealized_pnl,
            equity=equity,
            peak_equity=self._peak_equity,
        )

    def equity(self, unrealized_pnl: float = 0.0) -> float:
        return self.stats(unrealized_pnl=unrealized_pnl).equity