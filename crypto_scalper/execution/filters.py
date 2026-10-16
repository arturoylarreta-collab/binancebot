"""Per-symbol exchange filters (tick size, lot step, minimums).

Binance rejects any order whose price is not a multiple of ``tickSize``
(-1111/-4014), whose quantity is not a multiple of ``stepSize`` or below
``minQty`` (-4003/-1013) and whose notional is below ``MIN_NOTIONAL``
(-4164). Every layer that produces a price or a quantity therefore snaps to
the filters of *that* symbol, loaded from ``/fapi/v1/exchangeInfo``.

Rounding uses ``Decimal`` so ``0.1 + 0.2``-style float noise can never
produce an off-grid value.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any, Dict, Iterable, Optional


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    tick_size: float = 0.01
    step_size: float = 0.001
    min_qty: float = 0.001
    min_notional: float = 5.0
    market_step_size: float = 0.0   # MARKET_LOT_SIZE (0 → same as step_size)
    market_max_qty: float = 0.0     # 0 → unbounded

    def round_price(self, price: float, direction: str = "nearest") -> float:
        return _snap(price, self.tick_size, direction)

    def floor_qty(self, qty: float, market: bool = True) -> float:
        step = self.market_step_size if (market and self.market_step_size > 0) else self.step_size
        q = _snap(qty, step, "down")
        if market and self.market_max_qty > 0:
            q = min(q, _snap(self.market_max_qty, step, "down"))
        return q

    def check(self, qty: float, price: float) -> Optional[str]:
        """Return a rejection reason, or None when the order is tradable."""
        if qty < self.min_qty - 1e-12:
            return f"qty {qty} < minQty {self.min_qty}"
        if qty * price < self.min_notional - 1e-9:
            return f"notional {qty * price:.4f} < minNotional {self.min_notional}"
        return None

    def price_decimals(self) -> int:
        return _decimals(self.tick_size)

    def qty_decimals(self) -> int:
        return _decimals(self.step_size)


class FilterRegistry:
    """Symbol → SymbolFilters with a conservative fallback for unknown symbols."""

    def __init__(self, filters: Optional[Iterable[SymbolFilters]] = None,
                 default: Optional[SymbolFilters] = None) -> None:
        self._by_symbol: Dict[str, SymbolFilters] = {f.symbol: f for f in (filters or ())}
        self._default = default

    def __contains__(self, symbol: str) -> bool:
        return symbol in self._by_symbol

    def __len__(self) -> int:
        return len(self._by_symbol)

    def get(self, symbol: str) -> SymbolFilters:
        f = self._by_symbol.get(symbol)
        if f is not None:
            return f
        if self._default is not None:
            return SymbolFilters(symbol=symbol, **{
                k: getattr(self._default, k) for k in (
                    "tick_size", "step_size", "min_qty", "min_notional",
                    "market_step_size", "market_max_qty")
            })
        return SymbolFilters(symbol=symbol)

    def update(self, filters: Iterable[SymbolFilters]) -> None:
        for f in filters:
            self._by_symbol[f.symbol] = f

    @classmethod
    def from_exchange_info(cls, info: Dict[str, Any],
                           symbols: Optional[Iterable[str]] = None) -> "FilterRegistry":
        wanted = {s.upper() for s in symbols} if symbols else None
        out = []
        for sym in info.get("symbols", []):
            name = str(sym.get("symbol", "")).upper()
            if not name or (wanted is not None and name not in wanted):
                continue
            out.append(parse_symbol_filters(sym))
        return cls(out)


def parse_symbol_filters(sym: Dict[str, Any]) -> SymbolFilters:
    """Parse one ``exchangeInfo.symbols[]`` entry."""
    by_type = {f.get("filterType"): f for f in sym.get("filters", [])}
    price_f = by_type.get("PRICE_FILTER", {})
    lot_f = by_type.get("LOT_SIZE", {})
    mkt_f = by_type.get("MARKET_LOT_SIZE", {})
    notional_f = by_type.get("MIN_NOTIONAL", {})
    return SymbolFilters(
        symbol=str(sym["symbol"]).upper(),
        tick_size=float(price_f.get("tickSize", 0.01)),
        step_size=float(lot_f.get("stepSize", 0.001)),
        min_qty=float(lot_f.get("minQty", 0.001)),
        min_notional=float(notional_f.get("notional", notional_f.get("minNotional", 5.0))),
        market_step_size=float(mkt_f.get("stepSize", 0.0)),
        market_max_qty=float(mkt_f.get("maxQty", 0.0)),
    )


def format_decimal(value: float, step: float) -> str:
    """Render ``value`` with exactly the decimals implied by ``step`` (API-safe)."""
    d = _decimals(step)
    q = Decimal(str(value)).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    return format(q, "f")


def _snap(value: float, step: float, direction: str) -> float:
    if step <= 0:
        return float(value)
    rounding = {"down": ROUND_FLOOR, "up": ROUND_CEILING}.get(direction, ROUND_HALF_UP)
    d_step = Decimal(str(step))
    raw = Decimal(str(value)) / d_step
    nearest = raw.quantize(Decimal(1), rounding=ROUND_HALF_UP)
    if abs(raw - nearest) < Decimal("1e-9"):
        units = nearest  # float noise (0.8999999999999999) is not a real remainder
    else:
        units = raw.quantize(Decimal(1), rounding=rounding)
    return float(units * d_step)


def _decimals(step: float) -> int:
    if step <= 0:
        return 8
    exp = Decimal(str(step)).normalize().as_tuple().exponent
    return max(0, -int(exp))
