"""SQLite repository (FASE 7) — idempotent trading audit trail.

Everything the paper engine needs to reconstruct history and prove what
happened: positions (upsert by position_id), order reports (upsert by
client_order_id), fills (deduplicated in-process), risk decisions and
reconciliation reports. Feature snapshots and generic events land in the
same file so a paper session is fully auditable offline.

All writes go through a single thread lock; the workload is low-frequency
(one write per order/position/snapshot), so plain sqlite3 is sufficient
without any extra dependency.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from crypto_scalper.core.enums import PositionStatus
from crypto_scalper.core.models import (
    ExecutionReport,
    FeatureSnapshot,
    Fill,
    ManagedPosition,
    RiskDecision,
)
from crypto_scalper.execution.reconciliation import ReconciliationIssue, ReconciliationReport
from crypto_scalper.storage.base import Repository

_SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    ordered_quantity REAL NOT NULL,
    entry_price REAL NOT NULL,
    stop_loss_price REAL NOT NULL,
    take_profit_price REAL NOT NULL,
    notional_value REAL NOT NULL,
    risk_amount REAL NOT NULL,
    regime TEXT NOT NULL,
    status TEXT NOT NULL,
    entry_client_order_id TEXT NOT NULL,
    stop_client_order_id TEXT NOT NULL DEFAULT '',
    take_profit_client_order_id TEXT NOT NULL DEFAULT '',
    entry_order_id TEXT NOT NULL DEFAULT '',
    opened_ts_ms INTEGER NOT NULL,
    closed_ts_ms INTEGER NOT NULL DEFAULT 0,
    realized_pnl REAL NOT NULL DEFAULT 0,
    close_reason TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    original_quantity REAL NOT NULL,
    executed_quantity REAL NOT NULL,
    avg_price REAL NOT NULL,
    reject_reason TEXT NOT NULL DEFAULT '',
    ts_ms INTEGER NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    ts_ms INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    fee REAL NOT NULL DEFAULT 0,
    fee_asset TEXT NOT NULL DEFAULT 'USDT'
);

CREATE TABLE IF NOT EXISTS risk_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    verdict TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    ok INTEGER NOT NULL,
    issues_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    score REAL NOT NULL,
    eligible INTEGER NOT NULL,
    rejection_reason TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS heartbeats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    equity REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    drawdown_pct REAL NOT NULL,
    total_exposure REAL NOT NULL,
    open_count INTEGER NOT NULL,
    trades_today INTEGER NOT NULL,
    uptime_ms INTEGER NOT NULL DEFAULT 0
);
"""


class SqliteRepository(Repository):
    def __init__(self, db_path: Union[str, Path]) -> None:
        self._db_path = Path(db_path)
        if self._db_path != Path(":memory:"):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._migrate()
            self._conn.commit()
        self._seen_fills: set = set()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def _migrate(self) -> None:
        """Idempotent additive migrations: never break databases from FASE 8."""
        self._ensure_column("positions", "pnl_unrealized", "REAL NOT NULL DEFAULT 0")

    def _ensure_column(self, table: str, column: str, decl: str) -> None:
        existing = {
            str(r["name"])
            for r in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")

    async def close(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.close()

    # ── generic pipeline persistence ──────────────────────────────────────────

    async def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        payload = json.dumps(snapshot.to_dict())
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (table_name, ts_ms, payload) VALUES (?, ?, ?)",
                ("feature_snapshots", snapshot.timestamp_ms, payload),
            )
            self._conn.commit()

    async def save_event(self, table: str, payload: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (table_name, ts_ms, payload) VALUES (?, ?, ?)",
                (table, _now_ms(), json.dumps(payload, default=str)),
            )
            self._conn.commit()

    # ── trading audit trail ───────────────────────────────────────────────────

    async def save_position(
        self, position: ManagedPosition, pnl_unrealized: Optional[float] = None
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO positions (
                    position_id, symbol, side, quantity, ordered_quantity,
                    entry_price, stop_loss_price, take_profit_price,
                    notional_value, risk_amount, regime, status,
                    entry_client_order_id, stop_client_order_id,
                    take_profit_client_order_id, entry_order_id,
                    opened_ts_ms, closed_ts_ms, realized_pnl, close_reason,
                    pnl_unrealized
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _position_row(position, pnl_unrealized),
            )
            self._conn.commit()

    async def save_order(self, report: ExecutionReport) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO orders (
                    client_order_id, order_id, symbol, side, order_type, status,
                    original_quantity, executed_quantity, avg_price,
                    reject_reason, ts_ms, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _order_row(report),
            )
            self._conn.commit()

    async def save_fills(self, fills: Tuple[Fill, ...]) -> None:
        if not fills:
            return
        rows = []
        for fill in fills:
            key = (fill.order_id, fill.ts_ms, fill.quantity, fill.price, fill.side)
            if key in self._seen_fills:
                continue
            self._seen_fills.add(key)
            rows.append(_fill_row(fill))
        if not rows:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO fills (
                    symbol, client_order_id, side, quantity, price,
                    ts_ms, order_id, fee, fee_asset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._conn.commit()

    async def save_reconciliation(self, report: ReconciliationReport) -> None:
        issues = [
            {
                "kind": i.kind,
                "symbol": i.symbol,
                "detail": i.detail,
                "severity": i.severity,
            }
            for i in report.issues
        ]
        with self._lock:
            self._conn.execute(
                "INSERT INTO reconciliations (ts_ms, ok, issues_json) VALUES (?, ?, ?)",
                (_now_ms(), 1 if report.ok else 0, json.dumps(issues)),
            )
            self._conn.commit()

    async def save_risk_decision(self, decision: RiskDecision) -> None:
        payload = json.dumps({
            "verifier": decision.verifier,
            "details": decision.details,
            "position_size": decision.position_size,
            "stop_loss_price": decision.stop_loss_price,
            "take_profit_price": decision.take_profit_price,
            "notional_value": decision.notional_value,
            "risk_amount": decision.risk_amount,
            "leverage_used": decision.leverage_used,
            "risk_checks": decision.risk_checks,
        })
        with self._lock:
            self._conn.execute(
                "INSERT INTO risk_decisions (symbol, ts_ms, verdict, reason, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                (decision.symbol, decision.ts_ms, decision.verdict, decision.reason, payload),
            )
            self._conn.commit()

    async def save_signal(self, signal, rejection_reason: str = "") -> None:
        payload = json.dumps(signal.to_dict(), default=str)
        with self._lock:
            self._conn.execute(
                "INSERT INTO signals (symbol, ts_ms, signal_type, score, eligible, "
                "rejection_reason, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    signal.symbol,
                    signal.timestamp_ms,
                    signal.signal_type.name,
                    float(signal.score),
                    1 if signal.eligible else 0,
                    rejection_reason or "",
                    payload,
                ),
            )
            self._conn.commit()

    async def save_heartbeat(self, snapshot) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO heartbeats (ts_ms, mode, status, equity, realized_pnl, "
                "unrealized_pnl, drawdown_pct, total_exposure, open_count, "
                "trades_today, uptime_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                _heartbeat_row(snapshot),
            )
            self._conn.commit()

    async def load_positions(self) -> List[ManagedPosition]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM positions ORDER BY opened_ts_ms"
            ).fetchall()
        return [_position_from_row(dict(r)) for r in rows]

    async def load_orders(self) -> List[ExecutionReport]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM orders ORDER BY ts_ms").fetchall()
        return [_order_from_row(dict(r)) for r in rows]

    # ── test / audit helpers ──────────────────────────────────────────────────

    def count_rows(self, table: str) -> int:
        with self._lock:
            row = self._conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        return int(row["n"])

    def reconciliation_rows(self) -> List[Tuple[int, bool, List[Dict[str, Any]]]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts_ms, ok, issues_json FROM reconciliations ORDER BY id"
            ).fetchall()
        return [(int(r["ts_ms"]), bool(r["ok"]), json.loads(r["issues_json"])) for r in rows]


# ── row mapping helpers ─────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(time.time() * 1000)


def _position_row(
    position: ManagedPosition, pnl_unrealized: Optional[float] = None
) -> Tuple[Any, ...]:
    return (
        position.position_id,
        position.symbol,
        position.side,
        position.quantity,
        position.ordered_quantity,
        position.entry_price,
        position.stop_loss_price,
        position.take_profit_price,
        position.notional_value,
        position.risk_amount,
        position.regime,
        position.status.name,
        position.entry_client_order_id,
        position.stop_client_order_id,
        position.take_profit_client_order_id,
        position.entry_order_id,
        position.opened_ts_ms,
        position.closed_ts_ms,
        position.realized_pnl,
        position.close_reason,
        float(pnl_unrealized if pnl_unrealized is not None else 0.0),
    )


def _heartbeat_row(snapshot) -> Tuple[Any, ...]:
    return (
        int(snapshot.ts_ms),
        snapshot.mode,
        snapshot.status,
        float(snapshot.equity),
        float(snapshot.realized_pnl),
        float(snapshot.unrealized_pnl),
        float(snapshot.drawdown_pct),
        float(snapshot.total_exposure),
        int(snapshot.open_count),
        int(snapshot.trades_today),
        int(snapshot.uptime_ms),
    )


def _position_from_row(r: Dict[str, Any]) -> ManagedPosition:
    return ManagedPosition(
        position_id=r["position_id"],
        symbol=r["symbol"],
        side=r["side"],
        quantity=float(r["quantity"]),
        ordered_quantity=float(r["ordered_quantity"]),
        entry_price=float(r["entry_price"]),
        stop_loss_price=float(r["stop_loss_price"]),
        take_profit_price=float(r["take_profit_price"]),
        notional_value=float(r["notional_value"]),
        risk_amount=float(r["risk_amount"]),
        regime=r["regime"],
        status=PositionStatus[r["status"]],
        entry_client_order_id=r["entry_client_order_id"],
        stop_client_order_id=r["stop_client_order_id"],
        take_profit_client_order_id=r["take_profit_client_order_id"],
        entry_order_id=r["entry_order_id"],
        opened_ts_ms=int(r["opened_ts_ms"]),
        closed_ts_ms=int(r["closed_ts_ms"]),
        realized_pnl=float(r["realized_pnl"]),
        close_reason=r["close_reason"],
    )


def _order_row(report: ExecutionReport) -> Tuple[Any, ...]:
    payload = json.dumps(_report_to_dict(report))
    return (
        report.client_order_id,
        report.order_id,
        report.symbol,
        report.side,
        report.order_type,
        report.status,
        report.original_quantity,
        report.executed_quantity,
        report.avg_price,
        report.reject_reason,
        report.ts_ms,
        payload,
    )


def _order_from_row(r: Dict[str, Any]) -> ExecutionReport:
    payload = json.loads(r["payload"])
    return _report_from_dict(payload)


def _report_to_dict(report: ExecutionReport) -> Dict[str, Any]:
    d = asdict(report)
    d["fills"] = [f for f in d.get("fills", [])]
    return d


def _report_from_dict(d: Dict[str, Any]) -> ExecutionReport:
    fills = tuple(Fill(**f) for f in d.get("fills", []))
    data = {k: v for k, v in d.items() if k != "fills"}
    return ExecutionReport(fills=fills, **data)


def _fill_row(fill: Fill) -> Tuple[Any, ...]:
    return (
        fill.symbol,
        fill.client_order_id,
        fill.side,
        fill.quantity,
        fill.price,
        fill.ts_ms,
        fill.order_id,
        fill.fee,
        fill.fee_asset,
    )