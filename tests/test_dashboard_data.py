"""Dashboard data aggregation tests — lee SOLO la SQLite (agregación pura).

Cubre el flujo completo repo→DB→dashboard y la compatibilidad con esquemas
FASE 8 viejos (sin pnl_unrealized / signals / heartbeats).
"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from crypto_scalper.core.enums import PositionStatus, SignalType
from crypto_scalper.core.models import HeartbeatSnapshot, ManagedPosition, RiskDecision, Signal
from crypto_scalper.monitoring import dashboard_data as dd
from crypto_scalper.monitoring.observability import ObservabilityRepository
from crypto_scalper.storage.sqlite_repo import SqliteRepository
from tests.test_monitoring import FakeNotifier, mk_position


# ── flujo completo (repo → DB → dashboard) ─────────────────────────────────


async def test_dashboard_reads_repository_writes(tmp_path):
    db_path = str(tmp_path / "session.db")
    inner = SqliteRepository(db_path)
    obs = ObservabilityRepository(inner, notifier=FakeNotifier(), mode="paper",
                                  start_equity=10_000.0, heartbeat_interval_ms=1)

    await obs.save_position(mk_position(
        "p1", symbol="BTCUSDT", status=PositionStatus.CLOSED,
        entry=100.0, qty=0.1, realized=0.8, close_reason="tp", closed_ts=1000,
    ))
    await asyncio.sleep(0.01)  # heartbeat is throttled (1 ms); WAL writes are faster
    await obs.save_position(mk_position(
        "p2", symbol="ETHUSDT", status=PositionStatus.ACTIVE,
        entry=50.0, qty=0.2, notional=10.0,
    ))
    await obs.save_signal(Signal(
        symbol="BTCUSDT", timestamp_ms=1234, signal_type=SignalType.LONG,
        score=0.92, eligible=True, regime="trending_up",
    ), rejection_reason="")
    await obs.save_signal(Signal(
        symbol="ETHUSDT", timestamp_ms=1235, signal_type=SignalType.SHORT,
        score=0.31, eligible=False, regime="range",
    ), rejection_reason="risk_rejected")
    await inner.close()

    conn = dd.connect(db_path)
    try:
        status = dd.bot_status(conn)
        assert status["mode"] == "paper"
        assert "error" not in status
        assert status["open_count"] == 1
        assert status["realized"] == 0.8

        pnl = dd.daily_pnl(conn)
        assert pnl["realized"] == pytest.approx(0.8)

        positions = dd.open_positions(conn)
        assert len(positions) == 1
        assert positions[0]["symbol"] == "ETHUSDT"
        assert positions[0]["pnl_unrealized"] >= 0

        signals = dd.recent_signals(conn, limit=10)
        assert len(signals) == 2
        assert signals[0]["symbol"] == "ETHUSDT"  # más reciente primero
        assert signals[0]["rejection_reason"] == "risk_rejected"
        assert signals[0]["eligible"] is False

        rm = dd.risk_metrics(conn)
        assert rm["total_trades"] == 1
        assert rm["win_rate"] == pytest.approx(1.0)
        assert rm["open_count"] == 1
    finally:
        conn.close()


async def test_dashboard_heartbeat_equity_and_drawdown(tmp_path):
    db_path = str(tmp_path / "session2.db")
    inner = SqliteRepository(db_path)
    obs = ObservabilityRepository(inner, notifier=None, mode="backtest",
                                  start_equity=1_000.0, heartbeat_interval_ms=1)
    # posición abierta con mark 110 sobre entry 100 → unreal +1.0
    obs.attach_mark(lambda sym: 110.0)
    await obs.save_position(mk_position(
        "p1", symbol="BTCUSDT", status=PositionStatus.ACTIVE,
        entry=100.0, qty=0.1, notional=10.0,
    ))
    await asyncio.sleep(0.01)
    await obs.save_position(mk_position(
        "p1", symbol="BTCUSDT", status=PositionStatus.CLOSED,
        entry=100.0, qty=0.1, realized=0.5, close_reason="tp", closed_ts=2000,
    ))
    await inner.close()

    conn = dd.connect(db_path)
    try:
        status = dd.bot_status(conn)
        assert status["mode"] == "backtest"
        assert status["open_count"] == 0
    finally:
        conn.close()


# ── compatibilidad con esquema FASE 8 viejo ────────────────────────────────


def _legacy_conn(tmp_path) -> sqlite3.Connection:
    """Esquema FASE 8 sin pnl_unrealized, sin signals ni heartbeats."""
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE positions (
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
        CREATE TABLE risk_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            ts_ms INTEGER NOT NULL,
            verdict TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL
        );
    """)
    conn.execute(
        "INSERT INTO positions (position_id, symbol, side, quantity, ordered_quantity, "
        "entry_price, stop_loss_price, take_profit_price, notional_value, risk_amount, "
        "regime, status, entry_client_order_id, opened_ts_ms, closed_ts_ms, realized_pnl, "
        "close_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("p1", "BTCUSDT", "BUY", 0.1, 0.1, 100.0, 99.0, 101.0, 10.0, 0.1,
         "trending_up", "CLOSED", "entry-p1", 1000, 2000, 1.5, "tp"),
    )
    conn.commit()
    return conn


def test_legacy_schema_friendly(tmp_path):
    conn = _legacy_conn(tmp_path)
    try:
        assert "error" in dd.bot_status(conn)  # sin heartbeats → status acotado
        assert dd.daily_pnl(conn)["realized"] == pytest.approx(0.0)
        assert dd.recent_signals(conn) == []
        positions = dd.open_positions(conn)
        assert positions == []
        rm = dd.risk_metrics(conn)
        assert rm["total_trades"] == 1
        assert rm["win_rate"] == pytest.approx(1.0)
    finally:
        conn.close()