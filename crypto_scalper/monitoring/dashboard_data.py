"""Dashboard data aggregation — pure SQLite (no Streamlit import, no queues).

Todas las funciones reciben una conexión `sqlite3.Connection` (o `path`) y
devuelven dict/tuples testeados sin levantar Streamlit ni HTTP.  Leen
directamente la DB que `SqliteRepository` escribe (la misma que el paper o
backtest usa).  Gracias a las migraciones aditivas son FASE-8 compatibles
(las columnas `pnl_unrealized`, la tabla `signals` y `heartbeats` pueden no
existir aún en datasets viejos).

El dashboard web lo sirve el propio bot (``crypto_scalper/server``) en
``http://<host>:<PORT>/`` y consume estas funciones vía ``/api/*``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def connect(db_path: str) -> sqlite3.Connection:
    """Conexión readonly WAL para lecturas concurrentes (URI file:///)."""
    path = str(Path(db_path).resolve()).replace("\\", "/")
    conn = sqlite3.connect(f"file:///{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"] == column for row in rows)
    except Exception:
        return False


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    try:
        r = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return r is not None
    except Exception:
        return False


# ── heartbeat / bot status ──────────────────────────────────────────────────


def bot_status(conn: sqlite3.Connection) -> Dict[str, object]:
    """Last heartbeat + basic status."""
    if not _has_table(conn, "heartbeats"):
        return {"error": "no heartbeats table"}
    row = conn.execute(
        "SELECT * FROM heartbeats ORDER BY ts_ms DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return {"error": "no heartbeat recorded"}
    row = dict(row)
    return {
        "mode": row.get("mode", "unknown"),
        "status": row.get("status", "unknown"),
        "equity": float(row.get("equity", 0.0)),
        "realized": float(row.get("realized_pnl", 0.0)),
        "unrealized": float(row.get("unrealized_pnl", 0.0)),
        "drawdown_pct": float(row.get("drawdown_pct", 0.0)),
        "open_count": int(row.get("open_count", 0)),
        "trades_today": int(row.get("trades_today", 0)),
        "uptime_ms": int(row.get("uptime_ms", 0)),
    }


# ── daily PnL ───────────────────────────────────────────────────────────────


def daily_pnl(conn: sqlite3.Connection) -> Dict[str, float]:
    """Realized + unrealized + today trades + drawdown desde el último heartbeat."""
    latest = bot_status(conn)
    if "error" in latest:
        return {"realized": 0.0, "unrealized": 0.0, "trades_today": 0, "drawdown_pct": 0.0}
    return {
        "realized": float(latest["realized"]),
        "unrealized": float(latest["unrealized"]),
        "trades_today": int(latest["trades_today"]),
        "drawdown_pct": float(latest["drawdown_pct"]),
    }


# ── open positions ──────────────────────────────────────────────────────────


def open_positions(conn: sqlite3.Connection) -> List[Dict[str, object]]:
    """Positions currently in an open status."""
    rows = conn.execute(
        "SELECT * FROM positions WHERE status NOT IN ('CLOSED','ABORTED') "
        "ORDER BY opened_ts_ms DESC"
    ).fetchall()
    return [_position_dict(r) for r in rows]


def _position_dict(r: sqlite3.Row) -> Dict[str, object]:
    d = dict(r)
    return {
        "position_id": d.get("position_id", ""),
        "symbol": d.get("symbol", ""),
        "side": d.get("side", ""),
        "entry_price": float(d.get("entry_price", 0.0)),
        "quantity": float(d.get("quantity", 0.0)),
        "notional_value": float(d.get("notional_value", 0.0)),
        "stop_loss_price": float(d.get("stop_loss_price", 0.0)),
        "take_profit_price": float(d.get("take_profit_price", 0.0)),
        "realized_pnl": float(d.get("realized_pnl", 0.0)),
        "pnl_unrealized": float(d.get("pnl_unrealized", 0.0)) if "pnl_unrealized" in d else 0.0,
        "status": d.get("status", ""),
    }


# ── recent signals ──────────────────────────────────────────────────────────


def recent_signals(conn: sqlite3.Connection, limit: int = 10) -> List[Dict[str, object]]:
    """Last N signals (most recent first). Empty if table missing."""
    if not _has_table(conn, "signals"):
        return []
    rows = conn.execute(
        "SELECT * FROM signals ORDER BY ts_ms DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "symbol": dict(r).get("symbol", ""),
            "signal_type": dict(r).get("signal_type", ""),
            "score": float(dict(r).get("score", 0.0)),
            "eligible": bool(dict(r).get("eligible", 0)),
            "rejection_reason": dict(r).get("rejection_reason", ""),
            "ts_ms": int(dict(r).get("ts_ms", 0)),
        }
        for r in rows
    ]


# ── risk metrics ────────────────────────────────────────────────────────────


def risk_metrics(conn: sqlite3.Connection) -> Dict[str, float]:
    """Aggregate risk statistics from closed positions."""
    rows = conn.execute(
        "SELECT realized_pnl, notional_value FROM positions WHERE status='CLOSED'"
    ).fetchall()
    if not rows:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
            "max_drawdown_pct": 0.0,
            "total_exposure": 0.0,
            "open_count": 0,
        }
    pnls = [float(dict(r).get("realized_pnl", 0.0)) for r in rows]
    notional_sum = sum(float(dict(r).get("notional_value", 0.0)) for r in rows)
    wins = sum(1 for p in pnls if p > 0)
    open_rows = conn.execute(
        "SELECT SUM(notional_value) AS s, COUNT(*) AS c "
        "FROM positions WHERE status NOT IN ('CLOSED','ABORTED')"
    ).fetchone()
    return {
        "total_trades": len(pnls),
        "win_rate": wins / len(pnls),
        "avg_pnl": sum(pnls) / len(pnls),
        "max_drawdown_pct": max_drawdown_pct(conn),
        "total_exposure": float(open_rows["s"] or 0.0),
        "open_count": int(open_rows["c"] or 0),
    }


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
