"""Offline SQLite fixtures using the bot's actual persisted schema."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from collections.abc import Iterator

import pytest

from crypto_scalper.monitoring import dashboard_data as dd
from crypto_scalper.storage.sqlite_repo import _SCHEMA

START = int(datetime(2026, 9, 22, tzinfo=timezone.utc).timestamp() * 1_000)
DAY = 86_400_000


def position(conn: sqlite3.Connection, pid: str, symbol: str = "BTCUSDT", *,
             side: str = "BUY", status: str = "CLOSED", pnl: float = 25,
             opened: int = START + 100, closed: int = START + 200,
             reason: str = "take_profit") -> None:
    conn.execute("""INSERT INTO positions VALUES
        (?, ?, ?, 2, 2, 100, 95, 112.5, 200, 10, 'range', ?, ?, ?, ?, '', ?, ?, ?, ?, 0)
    """, (pid, symbol, side, status, f"ENTRY-{pid}", f"SL-{pid}", f"TP-{pid}",
          opened, closed, pnl, reason))


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "sesión #1.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(_SCHEMA)
        conn.execute("ALTER TABLE positions ADD COLUMN pnl_unrealized REAL DEFAULT 0")
        conn.execute("PRAGMA journal_mode=WAL")
        position(conn, "a")
        position(conn, "b", "ETHUSDT", side="SELL", pnl=-5, closed=START + 300, reason="manual")
        position(conn, "outside", closed=START + DAY, pnl=10)
        position(conn, "open", "ETHUSDT", status="ACTIVE", closed=0, reason="", pnl=0)
        position(conn, "old_open", opened=START - DAY, status="ACTIVE", closed=0, reason="", pnl=0)
        conn.execute("""INSERT INTO orders VALUES
            ('TP-a', '123', 'BTCUSDT', 'SELL', 'TAKE_PROFIT_MARKET', 'FILLED',
             2, 2, 112.5, '', ?, '{}')""", (START + 200,))
        conn.executemany("""INSERT INTO heartbeats
            (ts_ms, mode, status, equity, realized_pnl, unrealized_pnl, drawdown_pct,
             total_exposure, open_count, trades_today, uptime_ms)
            VALUES (?, 'paper', 'running', ?, 20, 0, ?, 400, 2, 2, 1000)""",
            [(START + 500, 1020, 0.02), (START, 1000, 0), (START + 100, 1025, 0.01)])
        conn.executemany("""INSERT INTO signals
            (symbol, ts_ms, signal_type, score, eligible, rejection_reason, payload)
            VALUES (?, ?, ?, 80, ?, ?, '{}')""", [
                ("BTCUSDT", START + 100, "LONG", 1, ""),
                ("BTCUSDT", START + 200, "SHORT", 1, "MAX_DRAWDOWN"),
                ("ETHUSDT", START + 300, "SHORT", 0, "LOW_CONFIDENCE"),
                ("ETHUSDT", START + DAY - 1, "FLAT", 0, "flat_signal_not_eligible"),
                ("ETHUSDT", START + DAY, "LONG", 1, ""),
            ])
        conn.executemany("INSERT INTO events (table_name, ts_ms, payload) VALUES (?, ?, ?)", [
            ("feature_snapshots", START + 100, json.dumps({"symbol": "BTCUSDT", "token": "never-display-this"})),
            ("heartbeat", START + 200, "malformed JSON"),
        ])
        conn.execute("INSERT INTO risk_decisions (symbol, ts_ms, verdict, reason, payload) VALUES ('ETHUSDT', ?, 'REJECTED', 'MAX_DRAWDOWN', '{}')", (START + 300,))
        conn.execute("INSERT INTO reconciliations (ts_ms, ok, issues_json) VALUES (?, 0, ?)",
                     (START + 400, json.dumps([{"symbol": "BTCUSDT", "detail": "missing protection"}])))
        conn.commit()
    return path


@pytest.fixture
def conn(database: Path) -> Iterator[sqlite3.Connection]:
    with closing(dd.connect(str(database))) as connection:
        yield connection


@pytest.fixture
def selection() -> dd.DashboardFilters:
    return dd.DashboardFilters(START, START + DAY, mode="paper")
