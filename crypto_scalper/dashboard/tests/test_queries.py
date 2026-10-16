"""Financial aggregation, filter boundaries and reader isolation regressions."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import sqlite3
from unittest.mock import patch

import pytest

from crypto_scalper.monitoring import dashboard_data as dd


def test_closed_history_and_actual_exit_price(conn, selection):
    rows = dd.trade_history(conn, selection)
    assert [r["position_id"] for r in rows] == ["b", "a"]
    assert rows[0]["exit_price"] is None
    assert rows[1]["exit_price"] == 112.5
    assert [r["cumulative_pnl"] for r in dd.cumulative_pnl(conn, selection)] == [25, 20]
    assert dd.risk_metrics(conn, selection)["win_rate"] == 0.5


def test_persisted_exit_price_wins_and_zero_means_unknown(database, conn, selection):
    """FASE 9 schema: positions.exit_price, where 0 is the legacy 'unknown'."""
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("ALTER TABLE positions ADD COLUMN exit_price REAL NOT NULL DEFAULT 0")
        writer.execute("UPDATE positions SET exit_price=101.5 WHERE position_id='b'")
        writer.commit()
    rows = {r["position_id"]: r["exit_price"] for r in dd.trade_history(conn, selection)}
    assert rows == {"b": 101.5, "a": 112.5}  # persisted value; else the filled order
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("DELETE FROM orders")
        writer.execute("UPDATE positions SET exit_price=0")
        writer.commit()
    assert [r["exit_price"] for r in dd.trade_history(conn, selection)] == [None, None]


def test_filters_apply_to_every_query(conn, selection):
    btc = replace(selection, symbols=("BTCUSDT",))
    assert [r["position_id"] for r in dd.trade_history(conn, btc)] == ["a"]
    assert len(dd.recent_signals(conn, 100, btc)) == 2
    assert dd.risk_metrics(conn, btc)["realized_pnl"] == 25
    assert dd.exposure_by_symbol(conn, btc) == []  # old open position outside opening range
    assert len(dd.system_logs(conn, btc)) == 2  # matching event and reconciliation issue
    assert len(dd.equity_history(conn, btc)) == 3  # explicitly portfolio-wide
    assert dd.open_positions(conn, selection)[0]["position_id"] == "open"
    assert dd.risk_metrics(conn, selection)["total_exposure"] == 200
    assert dd.risk_metrics(conn, selection)["max_drawdown_pct"] == 0.02


def test_dates_include_last_millisecond_not_next_day(conn, selection):
    assert len(dd.recent_signals(conn, 100, selection)) == 4
    assert dd.filter_options(conn)["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert [r["equity"] for r in dd.equity_history(conn, selection)] == [1000, 1025, 1020]


def test_signal_eligibility_does_not_override_rejection(conn, selection):
    rows = dd.signal_distribution(conn, selection)
    assert sum(r["count"] for r in rows) == 4
    assert sum(r["count"] for r in rows if r["approved"]) == 1


def test_unknown_or_mixed_modes_are_not_guessed(database, conn, selection):
    assert dd.trade_history(conn, replace(selection, mode="live")) == []
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("UPDATE heartbeats SET mode='live' WHERE id=1")
        writer.commit()
    assert dd.trade_history(conn, selection) == []
    assert dd.recent_signals(conn, 100, selection) == []
    assert dd.system_logs(conn, selection) == []
    assert len(dd.equity_history(conn, selection)) == 2
    assert len(dd.trade_history(conn, replace(selection, mode=None))) == 2


def test_sql_parameters_reject_symbol_injection(conn, selection):
    injected = replace(selection, symbols=("BTCUSDT') OR 1=1 --",))
    assert dd.trade_history(conn, injected) == []
    assert dd.recent_signals(conn, 100, injected) == []
    assert dd.system_logs(conn, injected) == []


def test_reader_cannot_write_and_does_not_create_missing_db(conn, tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("DELETE FROM positions")
    missing = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        dd.connect(str(missing))
    assert not missing.exists()


def test_exposure_without_any_closed_trade(database, conn, selection):
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("DELETE FROM positions WHERE status='CLOSED'")
        writer.commit()
    result = dd.risk_metrics(conn, selection)
    assert result["total_trades"] == 0
    assert result["total_exposure"] == 200
    assert result["open_count"] == 1


def test_graph_sampling_preserves_totals_endpoints_and_maximum(conn, selection):
    with patch.object(dd, "MAX_POINTS", 1):
        equity = dd.equity_history(conn, selection)
        pnl = dd.cumulative_pnl(conn, selection)
    assert [r["equity"] for r in equity] == [1000, 1020]
    assert pnl[-1]["cumulative_pnl"] == 20
    assert dd.risk_metrics(conn, selection)["max_drawdown_pct"] == 0.02
    assert len(dd.trade_history(conn, selection, limit=1)) == 1


def test_logs_exclude_raw_payloads(conn, selection):
    rows = dd.system_logs(conn, selection)
    assert len(rows) == 4
    assert "never-display-this" not in str(rows)
    assert {r["source"] for r in rows} == {"Evento", "Riesgo", "Reconciliación"}


def test_empty_legacy_database_is_supported(tmp_path):
    path = tmp_path / "empty.db"
    sqlite3.connect(path).close()
    with closing(dd.connect(str(path))) as conn:
        assert dd.filter_options(conn)["min_ts_ms"] is None
        assert dd.trade_history(conn) == []
        assert dd.equity_history(conn) == []
        assert dd.cumulative_pnl(conn) == []
        assert dd.system_logs(conn) == []
        assert dd.risk_metrics(conn)["total_exposure"] == 0


def test_query_budget_interrupts_work_and_releases_handler(conn):
    with patch.object(dd, "QUERY_TIMEOUT_SECONDS", -1), pytest.raises(sqlite3.OperationalError, match="interrupted"):
        dd._rows(conn, "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT SUM(x) FROM n")
    assert dd._rows(conn, "SELECT 1 AS ok") == [{"ok": 1}]


def test_wal_writer_can_commit_while_reader_is_connected(database, conn, selection):
    before = dd.risk_metrics(conn, selection)["realized_pnl"]
    with closing(sqlite3.connect(database, timeout=0)) as writer:
        writer.execute("UPDATE positions SET realized_pnl=30 WHERE position_id='a'")
        writer.commit()
    assert dd.risk_metrics(conn, selection)["realized_pnl"] == before + 5
