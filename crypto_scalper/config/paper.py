"""Paper trading configuration (FASE 7).

Controls the fully simulated account: starting equity, a placeholder taker
fee, the periodic reconciliation cadence, the state-summary cadence, an
optional per-symbol trade guard and where the audit database lives.
No secret, no connection flag: paper is always offline and red.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PaperConfig:
    start_equity: float = 10_000.0
    fee_pct: float = 0.0004          # placeholder taker fee for one round trip
    reconcile_interval_s: float = 60.0
    summary_interval_s: float = 30.0
    max_open_per_symbol: int = 1     # paper guard: never stack the same symbol
    db_path: Path = Path("logs/paper.db")