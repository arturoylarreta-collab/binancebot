"""Dashboard data aggregation — pure SQLite (no Streamlit import, no queues).

Todas las funciones reciben una conexión `sqlite3.Connection` (o `path`) y
devuelven dict/tuples testeados sin levantar Streamlit ni HTTP.  Leen
directamente la DB que `SqliteRepository` escribe (la misma que el paper o
backtest usa).  Gracias a las migraciones aditivas son FASE-8 compatibles
(las columnas `pnl_unrealized`, la tabla `signals` y `heartbeats` pueden no
existir aún en datasets viejos).

Comando para arrancar el dashboard (desde la raíz del proyecto):

    streamlit run crypto_scalper/dashboard/app.py
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
        "max_drawdown_pct": 0.0,
        "total_exposure": float(open_rows["s"] or 0.0),
        "open_count": int(open_rows["c"] or 0),
    }