"""Symbol universe definitions (externalized, never hardcoded per-run).

The final monitored universe is produced at runtime by the UniverseSelector
(market_data/universe.py). This module only holds defaults and filters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple

# Quote assets prioritized when the selector ranks candidate base assets.
PREFERRED_QUOTES: Tuple[str, ...] = ("USDT", "FDUSD", "USDC")

# Hard exclusions: anything that should never be traded programmatically.
BLOCKED_SYMBOLS: Tuple[str, ...] = ()

# Statuses that disqualify an instrument from the universe.
UNTRADABLE_STATUSES: Tuple[str, ...] = ("PENDING_TRADING", "BREAK", "SETTLE")


@dataclass(frozen=True)
class SymbolRules:
    allowed_quotes: Tuple[str, ...] = PREFERRED_QUOTES
    blocked: Tuple[str, ...] = BLOCKED_SYMBOLS
    untradable_statuses: Tuple[str, ...] = UNTRADABLE_STATUSES
    custom_priority: List[str] = field(default_factory=list)