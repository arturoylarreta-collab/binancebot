"""Read-only, bounded SQLite queries for the independent dashboard process.

No engine, network, Streamlit or repository imports belong here. Filters use
bound SQL parameters. Dates are UTC milliseconds with an exclusive upper bound.
Connections are owned by the caller and must be explicitly closed.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

Row = dict[str, Any]
MAX_ROWS = 5_000
MAX_POINTS = 2_000
QUERY_TIMEOUT_SECONDS = 0.25
MODES = ("backtest", "paper", "live")
_CLOSED = "t.status = 'CLOSED' AND NULLIF(TRIM(t.close_reason), '') IS NOT NULL"
_OPEN = "t.status NOT IN ('CLOSED', 'ABORTED')"


@dataclass(frozen=True)
class DashboardFilters:
    """Immutable cache key; empty symbols and a null mode mean all records.

    Heartbeats describe the whole portfolio and cannot be filtered by symbol.
    Tables without a mode can only inherit a mode from a single-mode database.
    A specific mode in an ambiguous/legacy database returns no untagged rows.
    """

    start_ms: int | None = None
    end_ms: int | None = None
    symbols: tuple[str, ...] = ()
    mode: str | None = None

    def __post_init__(self) -> None:
        if self.mode is not None and self.mode not in MODES:
            raise ValueError("Modo no válido")
        if self.start_ms is not None and self.end_ms is not None:
            if self.start_ms >= self.end_ms:
                raise ValueError("El final del intervalo debe ser posterior al inicio")


def connect(db_path: str) -> sqlite3.Connection:
    """Open an existing file read-only, without migrations or journal changes.

    A short busy timeout avoids waiting for the writer. Each query also has a
    VM time budget. WAL must already be configured by the database owner for
    concurrent readers/writers without rollback-journal read locks.
    """
    conn = sqlite3.connect(
        f"{Path(db_path).expanduser().resolve().as_uri()}?mode=ro",
        uri=True, timeout=0.05, isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[Row]:
    """Materialize a short read and release its cursor/lock immediately."""
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1_000)
    try:
        cursor = conn.execute(sql, params)
        try:
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        conn.set_progress_handler(None, 0)


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return bool(_rows(conn, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)))


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    # Table names here are internal constants, never UI input.
    return {r["name"] for r in _rows(conn, f'PRAGMA table_info("{table}")')}


def _modes(conn: sqlite3.Connection) -> tuple[str, ...]:
    if not _has_table(conn, "heartbeats"):
        return ()
    return tuple(r["mode"] for r in _rows(conn, "SELECT DISTINCT mode FROM heartbeats ORDER BY mode"))


def _where(
    conn: sqlite3.Connection, table: str, filters: DashboardFilters,
    timestamp: str = "t.ts_ms", symbol: str | None = "t.symbol",
) -> tuple[str, list[Any]]:
    """Build parameterized predicates; SQL expressions are internal constants."""
    clauses, params = ["1=1"], []
    if filters.start_ms is not None:
        clauses.append(f"{timestamp} >= ?")
        params.append(filters.start_ms)
    if filters.end_ms is not None:
        clauses.append(f"{timestamp} < ?")
        params.append(filters.end_ms)
    if filters.symbols and symbol is not None:
        clauses.append(f"{symbol} IN ({','.join('?' for _ in filters.symbols)})")
        params.extend(filters.symbols)
    if filters.mode is not None:
        if "mode" in _columns(conn, table):
            clauses.append("t.mode = ?")
            params.append(filters.mode)
        elif _modes(conn) != (filters.mode,):
            clauses.append("0=1")
    return " AND ".join(clauses), params


def filter_options(conn: sqlite3.Connection) -> Row:
    """Return symbols, full UTC date bounds and mode provenance from SQLite."""
    symbols: set[str] = set()
    bounds: list[int] = []
    sources = {
        "positions": ("opened_ts_ms", "closed_ts_ms"),
        "signals": ("ts_ms",), "heartbeats": ("ts_ms",),
        "events": ("ts_ms",), "risk_decisions": ("ts_ms",),
        "reconciliations": ("ts_ms",),
    }
    for table, timestamps in sources.items():
        columns = _columns(conn, table)
        if "symbol" in columns:
            symbols.update(r["symbol"] for r in _rows(conn, f"SELECT DISTINCT symbol FROM {table}") if r["symbol"])
        for timestamp in timestamps:
            if timestamp in columns:
                row = _rows(conn, f"SELECT MIN({timestamp}) lo, MAX({timestamp}) hi FROM {table} WHERE {timestamp} > 0")[0]
                bounds.extend(int(v) for v in row.values() if v is not None)
    return {
        "symbols": sorted(symbols), "modes": _modes(conn),
        "min_ts_ms": min(bounds) if bounds else None,
        "max_ts_ms": max(bounds) if bounds else None,
        "journal_mode": _rows(conn, "PRAGMA journal_mode")[0]["journal_mode"],
    }


def bot_status(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> Row:
    """Latest portfolio heartbeat in the chosen date interval and mode."""
    if not _has_table(conn, "heartbeats"):
        return {"error": "No hay tabla de heartbeats"}
    where, params = _where(conn, "heartbeats", filters, symbol=None)
    rows = _rows(conn, f"SELECT t.* FROM heartbeats t WHERE {where} ORDER BY ts_ms DESC, id DESC LIMIT 1", params)
    if not rows:
        return {"error": "No hay heartbeats en el intervalo seleccionado"}
    row = rows[0]
    return {**row, "realized": row["realized_pnl"], "unrealized": row["unrealized_pnl"]}


def daily_pnl(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> Row:
    """Compatibility view of the last heartbeat (realized is lifetime, not daily)."""
    latest = bot_status(conn, filters)
    return {key: latest.get(key, 0.0) for key in ("realized", "unrealized", "trades_today", "drawdown_pct")}


def _sampled(conn: sqlite3.Connection, sql: str, params: Sequence[Any], partition: str = "") -> list[Row]:
    """Bound graph payloads, preserving first/last points after full aggregation.

    Sampling is by row number, not LIMIT before accumulation. Each portfolio
    mode is sampled separately. Intermediate peaks may be omitted; risk
    aggregates are calculated separately over the complete filtered period.
    """
    part = f"PARTITION BY {partition} " if partition else ""
    return _rows(conn, f"""
        WITH source AS ({sql}), numbered AS (
            SELECT *, ROW_NUMBER() OVER ({part}ORDER BY ts_ms, id) rn,
                   COUNT(*) OVER ({part.strip()}) total FROM source
        ) SELECT * FROM numbered
        WHERE (rn - 1) % MAX(1, (total + ? - 1) / ?) = 0 OR rn = total
        ORDER BY ts_ms, id
    """, [*params, MAX_POINTS, MAX_POINTS])


def equity_history(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> list[Row]:
    """Chronological portfolio equity/drawdown, keeping distinct modes separate."""
    if not _has_table(conn, "heartbeats"):
        return []
    where, params = _where(conn, "heartbeats", filters, symbol=None)
    return _sampled(conn, f"SELECT t.* FROM heartbeats t WHERE {where}", params, "mode")


def _position_dict(row: Row) -> Row:
    return {**row, "pnl_unrealized": row.get("pnl_unrealized", 0.0)}


def open_positions(
    conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters(), limit: int = 1_000,
) -> list[Row]:
    """Current open positions opened in the selected interval, never reconstructed history."""
    if not _has_table(conn, "positions"):
        return []
    where, params = _where(conn, "positions", filters, "t.opened_ts_ms")
    return [_position_dict(r) for r in _rows(conn, f"""
        SELECT t.* FROM positions t WHERE {_OPEN} AND {where}
        ORDER BY opened_ts_ms DESC, position_id DESC LIMIT ?
    """, [*params, max(1, min(limit, MAX_ROWS))])]


def trade_history(
    conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters(), limit: int = 1_000,
) -> list[Row]:
    """Closed trades by closing time, with actual exit order average when available.

    FASE 9 writers persist positions.exit_price (0 = unknown, the default for
    older rows). Otherwise match the exact SL/TP/manual close client ID;
    missing executed reports produce NULL, never a guessed price from PnL.
    Empty close_reason is the schema's open-position default and is excluded.
    """
    if not _has_table(conn, "positions"):
        return []
    where, params = _where(conn, "positions", filters, "t.closed_ts_ms")
    join, price = "", "NULL"
    if _has_table(conn, "orders"):
        join = """LEFT JOIN orders o ON o.client_order_id = CASE t.close_reason
            WHEN 'stop_loss' THEN t.stop_client_order_id
            WHEN 'take_profit' THEN t.take_profit_client_order_id
            ELSE 'CLOSE-' || UPPER(t.position_id) END
            AND o.symbol = t.symbol AND o.executed_quantity > 0 AND o.avg_price > 0
            AND o.side != t.side"""
        price = "o.avg_price"
    if _has_column(conn, "positions", "exit_price"):
        price = f"COALESCE(NULLIF(t.exit_price, 0), {price})"
    # A distinct alias: t.* already contains exit_price on FASE 9 schemas.
    rows = _rows(conn, f"""
        SELECT t.*, {price} AS resolved_exit_price FROM positions t {join}
        WHERE {_CLOSED} AND {where}
        ORDER BY t.closed_ts_ms DESC, t.position_id DESC LIMIT ?
    """, [*params, max(1, min(limit, MAX_ROWS))])
    for row in rows:
        row["exit_price"] = row.pop("resolved_exit_price")
    return rows


def cumulative_pnl(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> list[Row]:
    """Realized PnL accumulated from zero within the selected closing-date range."""
    if not _has_table(conn, "positions"):
        return []
    where, params = _where(conn, "positions", filters, "t.closed_ts_ms")
    return _sampled(conn, f"""
        SELECT closed_ts_ms ts_ms, position_id id, realized_pnl,
            SUM(realized_pnl) OVER (ORDER BY closed_ts_ms, position_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_pnl
        FROM positions t WHERE {_CLOSED} AND {where}
    """, params)


def recent_signals(
    conn: sqlite3.Connection, limit: int = 10, filters: DashboardFilters = DashboardFilters(),
) -> list[Row]:
    """Latest signals, retaining eligibility separately from downstream rejection."""
    if not _has_table(conn, "signals"):
        return []
    where, params = _where(conn, "signals", filters)
    return [{**r, "eligible": bool(r["eligible"])} for r in _rows(conn, f"""
        SELECT symbol, signal_type, score, eligible, rejection_reason, ts_ms
        FROM signals t WHERE {where} ORDER BY ts_ms DESC, id DESC LIMIT ?
    """, [*params, max(1, min(limit, MAX_ROWS))])]


def signal_distribution(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> list[Row]:
    """Count the full interval, including FLAT and eligible-but-rejected signals.

    'approved' here means eligible with no recorded rejection. It does not prove
    an order was submitted or filled; eligibility alone is not risk approval.
    """
    if not _has_table(conn, "signals"):
        return []
    where, params = _where(conn, "signals", filters)
    return _rows(conn, f"""
        SELECT signal_type, CASE WHEN eligible = 1
            AND COALESCE(TRIM(rejection_reason), '') = ''
            AND signal_type IN ('LONG', 'SHORT') THEN 1 ELSE 0 END approved,
            COUNT(*) count FROM signals t WHERE {where}
        GROUP BY signal_type, approved ORDER BY signal_type, approved
    """, params)


def risk_metrics(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> Row:
    """Aggregate all filtered trades/open positions, including accounts with no closes."""
    result: Row = dict(total_trades=0, win_rate=0.0, avg_pnl=0.0, realized_pnl=0.0,
                       max_drawdown_pct=0.0, total_exposure=0.0, open_count=0)
    if _has_table(conn, "positions"):
        where, params = _where(conn, "positions", filters, "t.closed_ts_ms")
        closed = _rows(conn, f"""
            SELECT COUNT(*) total_trades, COALESCE(AVG(realized_pnl > 0), 0) win_rate,
                COALESCE(AVG(realized_pnl), 0) avg_pnl,
                COALESCE(SUM(realized_pnl), 0) realized_pnl
            FROM positions t WHERE {_CLOSED} AND {where}
        """, params)[0]
        where, params = _where(conn, "positions", filters, "t.opened_ts_ms")
        opened = _rows(conn, f"""
            SELECT COUNT(*) open_count, COALESCE(SUM(ABS(notional_value)), 0) total_exposure
            FROM positions t WHERE {_OPEN} AND {where}
        """, params)[0]
        result.update(closed)
        result.update(opened)
    if _has_table(conn, "heartbeats"):
        where, params = _where(conn, "heartbeats", filters, symbol=None)
        result["max_drawdown_pct"] = _rows(conn, f"SELECT COALESCE(MAX(drawdown_pct), 0) dd FROM heartbeats t WHERE {where}", params)[0]["dd"]
    return result


def exposure_by_symbol(conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters()) -> list[Row]:
    """Gross persisted open notional by symbol/side, independent of the table row cap."""
    if not _has_table(conn, "positions"):
        return []
    where, params = _where(conn, "positions", filters, "t.opened_ts_ms")
    return _rows(conn, f"""
        SELECT symbol, side, SUM(ABS(notional_value)) exposure, COUNT(*) open_count
        FROM positions t WHERE {_OPEN} AND {where}
        GROUP BY symbol, side ORDER BY exposure DESC
    """, params)


def system_logs(
    conn: sqlite3.Connection, filters: DashboardFilters = DashboardFilters(), limit: int = 1_000,
) -> list[Row]:
    """SQLite audit events and risk decisions; never reads log files or raw secrets.

    Arbitrary event payloads are not returned. Only the event category and symbol
    are exposed; malformed JSON stays readable as an event with no symbol.
    Global reconciliation records are excluded when a symbol is selected.
    """
    queries: list[str] = []
    params: list[Any] = []
    if _has_table(conn, "events"):
        symbol = "CASE WHEN json_valid(t.payload) THEN json_extract(t.payload, '$.symbol') END"
        where, values = _where(conn, "events", filters, symbol=symbol)
        queries.append(f"SELECT ts_ms, id, 'Evento' source, {symbol} symbol, table_name detail FROM events t WHERE {where}")
        params.extend(values)
    if _has_table(conn, "risk_decisions"):
        where, values = _where(conn, "risk_decisions", filters)
        queries.append(f"SELECT ts_ms, id, 'Riesgo' source, symbol, verdict || ': ' || reason detail FROM risk_decisions t WHERE {where}")
        params.extend(values)
    if _has_table(conn, "reconciliations"):
        where, values = _where(conn, "reconciliations", filters, symbol=None)
        if filters.symbols:
            # Each issue has its own symbol; successful global checks have none.
            placeholders = ','.join('?' for _ in filters.symbols)
            where += f" AND EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(t.issues_json) THEN t.issues_json ELSE '[]' END) j WHERE json_extract(j.value, '$.symbol') IN ({placeholders}))"
            values.extend(filters.symbols)
        queries.append(f"SELECT ts_ms, id, 'Reconciliación' source, NULL symbol, CASE WHEN ok=1 THEN 'Correcta' ELSE 'Incidencias registradas' END detail FROM reconciliations t WHERE {where}")
        params.extend(values)
    if not queries:
        return []
    return _rows(conn, " UNION ALL ".join(queries) + " ORDER BY ts_ms DESC, source, id DESC LIMIT ?", [*params, max(1, min(limit, MAX_ROWS))])

def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return column in _columns(conn, table)


# ── FASE 9: richer analytics for the web dashboard ──────────────────────────


def max_drawdown_pct(conn: sqlite3.Connection) -> float:
    """Peak-to-trough drawdown over the whole heartbeat equity curve."""
    if not _has_table(conn, "heartbeats"):
        return 0.0
    peak = 0.0
    worst = 0.0
    for (equity,) in conn.execute("SELECT equity FROM heartbeats ORDER BY ts_ms"):
        e = float(equity or 0.0)
        peak = max(peak, e)
        if peak > 0:
            worst = max(worst, (peak - e) / peak)
    return worst


def equity_series(conn: sqlite3.Connection, since_ms: int = 0,
                  max_points: int = 600) -> List[Dict[str, float]]:
    """Heartbeat equity curve, evenly down-sampled to ``max_points``."""
    if not _has_table(conn, "heartbeats"):
        return []
    rows = conn.execute(
        "SELECT ts_ms, equity, realized_pnl, unrealized_pnl, drawdown_pct, open_count "
        "FROM heartbeats WHERE ts_ms >= ? ORDER BY ts_ms", (since_ms,)
    ).fetchall()
    if len(rows) > max_points:
        step = len(rows) / max_points
        rows = [rows[int(i * step)] for i in range(max_points)] + [rows[-1]]
    return [
        {"ts_ms": int(r[0]), "equity": float(r[1]), "realized": float(r[2]),
         "unrealized": float(r[3]), "drawdown_pct": float(r[4]), "open_count": int(r[5])}
        for r in rows
    ]


def closed_trades(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, object]]:
    has_fees = _has_column(conn, "positions", "fees")
    has_exit = _has_column(conn, "positions", "exit_price")
    rows = conn.execute(
        "SELECT * FROM positions WHERE status='CLOSED' ORDER BY closed_ts_ms DESC LIMIT ?",
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        fees = float(d.get("fees", 0.0)) if has_fees else 0.0
        gross = float(d.get("realized_pnl", 0.0))
        opened = int(d.get("opened_ts_ms", 0))
        closed = int(d.get("closed_ts_ms", 0))
        out.append({
            "position_id": d.get("position_id", ""),
            "symbol": d.get("symbol", ""),
            "side": "LONG" if d.get("side") == "BUY" else "SHORT",
            "quantity": float(d.get("quantity", 0.0)),
            "entry_price": float(d.get("entry_price", 0.0)),
            "exit_price": float(d.get("exit_price", 0.0)) if has_exit else 0.0,
            "stop_loss_price": float(d.get("stop_loss_price", 0.0)),
            "take_profit_price": float(d.get("take_profit_price", 0.0)),
            "gross_pnl": gross,
            "fees": fees,
            "net_pnl": gross - fees,
            "reason": d.get("close_reason", ""),
            "regime": d.get("regime", ""),
            "opened_ts_ms": opened,
            "closed_ts_ms": closed,
            "duration_s": max(0, (closed - opened) // 1000),
        })
    return out


def performance_stats(conn: sqlite3.Connection) -> Dict[str, object]:
    """Net-of-fee trading statistics over every closed position."""
    trades = closed_trades(conn, limit=1_000_000)
    nets = [t["net_pnl"] for t in trades]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    n = len(nets)

    def _group(key: str) -> Dict[str, Dict[str, float]]:
        g: Dict[str, Dict[str, float]] = {}
        for t in trades:
            k = str(t[key] or "unknown")
            e = g.setdefault(k, {"trades": 0, "wins": 0, "net_pnl": 0.0})
            e["trades"] += 1
            e["wins"] += 1 if t["net_pnl"] > 0 else 0
            e["net_pnl"] += t["net_pnl"]
        for e in g.values():
            e["win_rate"] = e["wins"] / e["trades"] if e["trades"] else 0.0
        return g

    streak = worst_streak = 0
    for x in reversed(nets):  # chronological order
        streak = streak + 1 if x <= 0 else 0
        worst_streak = max(worst_streak, streak)
    if gross_loss > 0:
        pf = gross_win / gross_loss
    else:
        pf = None if gross_win > 0 else 0.0   # None = infinite (no losses yet)
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n if n else 0.0,
        "net_pnl": sum(nets),
        "gross_pnl": sum(t["gross_pnl"] for t in trades),
        "fees": sum(t["fees"] for t in trades),
        "avg_win": gross_win / len(wins) if wins else 0.0,
        "avg_loss": -gross_loss / len(losses) if losses else 0.0,
        "profit_factor": pf,
        "expectancy": sum(nets) / n if n else 0.0,
        "best": max(nets) if nets else 0.0,
        "worst": min(nets) if nets else 0.0,
        "avg_duration_s": (sum(t["duration_s"] for t in trades) / n) if n else 0.0,
        "max_losing_streak": worst_streak,
        "max_drawdown_pct": max_drawdown_pct(conn),
        "by_symbol": _group("symbol"),
        "by_reason": _group("reason"),
    }


def rejection_breakdown(conn: sqlite3.Connection, since_ms: int = 0,
                        limit: int = 12) -> List[Dict[str, object]]:
    """Why directional signals did not become trades (risk + orchestrator)."""
    if not _has_table(conn, "signals"):
        return []
    rows = conn.execute(
        "SELECT rejection_reason, COUNT(*) FROM signals "
        "WHERE ts_ms >= ? AND signal_type != 'FLAT' GROUP BY rejection_reason",
        (since_ms,),
    ).fetchall()
    merged: Dict[str, int] = {}
    for reason, count in rows:
        # collapse per-symbol suffixes, e.g. symbol_already_open:BTCUSDT
        key = (str(reason or "") or "executed").split(":")[0][:60]
        merged[key] = merged.get(key, 0) + int(count)
    ordered = sorted(merged.items(), key=lambda kv: -kv[1])[:limit]
    return [{"reason": k, "count": v} for k, v in ordered]


def signal_series(conn: sqlite3.Connection, symbol: str, since_ms: int = 0,
                  max_points: int = 400) -> List[Dict[str, object]]:
    if not _has_table(conn, "signals"):
        return []
    rows = conn.execute(
        "SELECT ts_ms, score, signal_type, rejection_reason FROM signals "
        "WHERE symbol=? AND ts_ms >= ? ORDER BY ts_ms", (symbol, since_ms)
    ).fetchall()
    if len(rows) > max_points:
        step = len(rows) / max_points
        rows = [rows[int(i * step)] for i in range(max_points)]
    return [{"ts_ms": int(r[0]), "score": float(r[1]), "type": r[2], "reason": r[3]} for r in rows]


def recent_reconciliations(conn: sqlite3.Connection, limit: int = 5) -> List[Dict[str, object]]:
    if not _has_table(conn, "reconciliations"):
        return []
    import json as _json
    rows = conn.execute(
        "SELECT ts_ms, ok, issues_json FROM reconciliations ORDER BY ts_ms DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for r in rows:
        try:
            issues = _json.loads(r[2] or "[]")
        except ValueError:
            issues = []
        out.append({"ts_ms": int(r[0]), "ok": bool(r[1]), "issues": issues})
    return out
