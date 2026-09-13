"""Historical kline loading, replay and OHLC→tick interpolation (FASE 8).

Fuentes:

  * `BinanceKlinesDownloader` — página el endpoint público /fapi/v1/klines a
    través del cliente REST existente y normaliza a `Candle`. El store
    canónico OFF-LINE es el CSV por símbolo (sin pandas, solo `csv` estándar).
  * CSV por símbolo → `load_klines_csv` (el mismo formato que escribe
    `save_klines_csv`).

`kline_to_trades` sintetiza un camino intrabar determinista O→H→L→C como
trades aggTrade de forma que el FeatureEngine REAL y el CandleBuilder REAL
computen las features como en vivo. `synthetic_orderbook` reconstruye un book
de 2 niveles por barra (documentado como asunción) para que `is_ready()` sea
cierto y el spread no sea cero; insisto: **nunca** se usa información futura.

`KlineCorpus` intercala por símbolo las series en orden cronológico estricto
(ties estables por nombre de símbolo).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from crypto_scalper.core.enums import AggressorSide
from crypto_scalper.core.models import AggTrade, Candle
from crypto_scalper.core.exceptions import DataIntegrityError
from crypto_scalper.market_data.rest import BinanceFuturesRest

_CSV_FIELDS = (
    "symbol", "open_time_ms", "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count",
)

# Intervalos de kline admitidos por la API de Binance Futures (USDⓈ-M).
_INTERVAL_PARAMS = {
    1: "1s",
    60: "1m",
    180: "3m",
    300: "5m",
    900: "15m",
    1800: "30m",
    3600: "1h",
    14400: "4h",
    86400: "1d",
}


def interval_to_param(interval_s: int) -> str:
    if interval_s not in _INTERVAL_PARAMS:
        raise ValueError(f"unsupported kline interval_s: {interval_s}")
    return _INTERVAL_PARAMS[interval_s]


def interval_to_seconds(interval: str) -> int:
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        return int(interval[:-1]) * multipliers[interval[-1]]
    except (KeyError, TypeError, ValueError):
        raise ValueError(f"unsupported kline interval: {interval!r}")


# ── Parsing / serialization ────────────────────────────────────────────────────


def _raw_row_to_candle(row: List[Any], symbol: str, interval_s: int) -> Candle:
    return Candle(
        symbol=symbol,
        ts_ms=int(row[0]),
        interval_s=interval_s,
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
        completed=True,
    )


def parse_klines(
    rows: List[List[Any]], symbol: str, interval_s: int = 60
) -> List[Candle]:
    """Normaliza filas crudas de /fapi/v1/klines a `Candle` ordenados por ts."""
    candles = [_raw_row_to_candle(r, symbol, interval_s) for r in rows]
    candles = _validate_and_sort(candles)
    return candles


def save_klines_csv(path: Path, candles: List[Candle]) -> None:
    """Persiste candles al CSV canónico (overwrites)."""
    path = Path(path)
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(_CSV_FIELDS))
        writer.writeheader()
        for c in candles:
            writer.writerow({
                "symbol": c.symbol,
                "open_time_ms": c.ts_ms,
                "open": f"{c.open:.10g}",
                "high": f"{c.high:.10g}",
                "low": f"{c.low:.10g}",
                "close": f"{c.close:.10g}",
                "volume": f"{c.volume:.10g}",
                "quote_volume": f"{c.quote_volume:.10g}",
                "trade_count": c.trade_count,
            })


def load_klines_csv(path: Path, symbol: str, interval_s: int = 60) -> List[Candle]:
    """Carga candles del CSV canónico; falla si el archivo está vacío o corrupto."""
    path = Path(path)
    candles: List[Candle] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            row_symbol = (row.get("symbol") or symbol).upper()
            candles.append(Candle(
                symbol=row_symbol,
                ts_ms=int(row["open_time_ms"]),
                interval_s=interval_s,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                quote_volume=float(row["quote_volume"]),
                trade_count=int(row["trade_count"]),
                completed=True,
            ))
    candles = _validate_and_sort(candles)
    if not candles:
        raise DataIntegrityError(f"no candles in {path}")
    return candles


def klines_csv_path(data_dir: Path, symbol: str, interval_s: int) -> Path:
    return Path(data_dir) / f"{symbol}_{interval_to_param(interval_s)}.csv"


def _validate_and_sort(candles: List[Candle]) -> List[Candle]:
    candles = [c for c in candles if c.volume >= 0.0]
    candles.sort(key=lambda c: (c.ts_ms, c.symbol))
    unique: List[Candle] = []
    seen_ts = set()
    for c in candles:
        if c.ts_ms in seen_ts:
            continue
        seen_ts.add(c.ts_ms)
        unique.append(c)
    return unique


# ── Deterministic intra-bar interpolation ──────────────────────────────────────


def kline_to_trades(
    symbol: str, candle: Candle, steps: Optional[int] = None
) -> List[AggTrade]:
    """Convierte una kline a trades sintéticos con camino O→H→L→C.

    El camino es determinista (no RNG): 40% de los pasos suben O→H, 30%
    bajan H→L y el resto L→C. Cada precio queda acotado dentro de [low, high]
    y el quote volume agregado coincide con la kline.
    """
    if steps is None:
        steps = min(30, max(6, candle.interval_s))
    steps = max(2, int(steps))
    span_ms = candle.interval_s * 1000
    step_ms = max(1, span_ms // steps)
    o, h, l, c = candle.open, candle.high, candle.low, candle.close

    i_peak = max(1, int(steps * 0.4))
    i_valley = max(i_peak + 1, min(steps - 1, i_peak + int(steps * 0.3)))

    prices: List[float] = []
    for i in range(1, steps + 1):
        if i <= i_peak:
            px = o + (h - o) * (i / i_peak)
        elif i <= i_valley:
            px = h + (l - h) * ((i - i_peak) / max(1, (i_valley - i_peak)))
        else:
            px = l + (c - l) * ((i - i_valley) / max(1, (steps - i_valley)))
        prices.append(max(l, min(h, px)))

    per_trade = candle.volume / steps if steps else 0.0
    trades: List[AggTrade] = []
    prev = o
    for i, px in enumerate(prices, start=1):
        ts = candle.ts_ms + i * step_ms
        trades.append(AggTrade(
            symbol=symbol,
            event_time_ms=ts,
            trade_id=candle.ts_ms + i,
            price=px,
            quantity=max(0.0, per_trade),
            aggressor=AggressorSide.BUY if px >= prev else AggressorSide.SELL,
        ))
        prev = px
    return trades


def synthetic_orderbook(
    candle: Candle, levels: int = 2
) -> Tuple[List[List[float]], List[List[float]]]:
    """Book sintético de 2 niveles por barra alrededor del rango de la kline.

    Asunción documentada: sin feeds L2 históricos, reconstruimos un book
    centrado en el close solo para que OBI/spread/microprice estén definidos;
    jamás transportan información del futuro (se derivan de la propia barra,
    ya pasada, y del volume de la barra).
    """
    half_range = max(1e-9, (candle.high - candle.low) * 0.25)
    qty = candle.volume / max(1, levels)
    bids = [[round(candle.close - half_range * k, 8), qty] for k in range(1, levels + 1)]
    asks = [[round(candle.close + half_range * k, 8), qty] for k in range(1, levels + 1)]
    return bids, asks


# ── Corpus / replay ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BarEvent:
    symbol: str
    candle: Candle


class KlineCorpus:
    """Serie multi-símbolo de klines disponibles para el backtest."""

    def __init__(self, candles_by_symbol: Dict[str, List[Candle]]) -> None:
        if not candles_by_symbol:
            raise ValueError("KlineCorpus requires at least one symbol")
        normalized: Dict[str, List[Candle]] = {}
        for symbol, candles in candles_by_symbol.items():
            candles = _validate_and_sort(list(candles))
            if not candles:
                raise DataIntegrityError(f"{symbol}: empty kline series")
            intervals = {c.interval_s for c in candles}
            if len(intervals) != 1:
                raise DataIntegrityError(
                    f"{symbol}: mixed interval_s in corpus -> {sorted(intervals)}"
                )
            normalized[symbol.upper()] = candles
        self._symbols = sorted(normalized)
        self._candles = normalized
        self._interval_s = normalized[self._symbols[0]][0].interval_s

    @property
    def symbols(self) -> Tuple[str, ...]:
        return tuple(self._symbols)

    @property
    def interval_s(self) -> int:
        return self._interval_s

    def candles(self, symbol: str) -> List[Candle]:
        return self._candles[symbol.upper()]

    @property
    def start_ms(self) -> int:
        return min(self._candles[s][0].ts_ms for s in self._symbols)

    @property
    def end_ms(self) -> int:
        return max(self._candles[s][-1].ts_ms for s in self._symbols)

    def __len__(self) -> int:
        return sum(len(cs) for cs in self._candles.values())

    def events(self) -> Iterator[BarEvent]:
        """Intercala cronológicamente las klines de todos los símbolos."""
        idx = {s: 0 for s in self._symbols}
        remaining = len(self)
        while remaining:
            best: Optional[str] = None
            best_ts = None
            for s in self._symbols:
                i = idx[s]
                if i >= len(self._candles[s]):
                    continue
                ts = self._candles[s][i].ts_ms
                if best is None or ts < best_ts:
                    best, best_ts = s, ts
            if best is None:
                break
            candle = self._candles[best][idx[best]]
            idx[best] += 1
            remaining -= 1
            yield BarEvent(symbol=best, candle=candle)


# ── Remote downloader (paginado, backward-compatible) ─────────────────────────


class BinanceKlinesDownloader:
    """Descarga histórico paginado de klines al store CSV local.

    FASE 8: sin rumores — el paginado usa `startTime` con `endTime` para que
    cada página devuelva solo barras dentro del rango pedido.
    """

    def __init__(
        self,
        rest: BinanceFuturesRest,
        interval_s: int = 60,
        page_size: int = 1000,
    ) -> None:
        self._rest = rest
        self._interval_s = interval_s
        self._interval = interval_to_param(interval_s)
        self._page_size = page_size

    async def download(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> List[Candle]:
        if start_ms > end_ms:
            return []
        interval_ms = self._interval_s * 1000
        cursor = _align_up(start_ms, interval_ms)
        rows: List[List[Any]] = []
        while cursor <= end_ms:
            page = await self._rest.klines(
                symbol,
                interval=self._interval,
                limit=self._page_size,
                start_time=cursor,
                end_time=end_ms,
            )
            if not page:
                break
            rows.extend(page)
            page_open = int(page[0][0])
            next_cursor = _align_up(int(page[-1][0]) + interval_ms, interval_ms)
            if next_cursor <= cursor or next_cursor <= page_open:
                break  # sin progreso: evita bucle infinito
            cursor = next_cursor
        return parse_klines(rows, symbol.upper(), self._interval_s)


def _align_up(ts_ms: int, interval_ms: int) -> int:
    return (ts_ms // interval_ms) * interval_ms