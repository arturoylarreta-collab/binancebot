"""Offline Streamlit navigation, caching and recovery tests; no server or Binance."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3
import time
from unittest.mock import patch

import pytest
from streamlit.testing.v1 import AppTest

from crypto_scalper.dashboard import common
from crypto_scalper.monitoring import dashboard_data as dd

APP = Path(__file__).resolve().parents[1] / "app.py"


@pytest.fixture(autouse=True)
def clear_cache():
    common.load_data.clear()
    common.load_options.clear()
    yield
    common.load_data.clear()
    common.load_options.clear()


def test_date_filter_is_inclusive_in_utc():
    filters = common.date_filters(date(2026, 9, 22), date(2026, 9, 22), [], None)
    assert filters.end_ms - filters.start_ms == 86_400_000
    assert common.timestamp(filters.start_ms).isoformat() == "2026-09-22T00:00:00+00:00"


def test_navigation_preserves_filters_and_tables_are_typed(database, monkeypatch):
    monkeypatch.setenv("DASHBOARD_DB_PATH", str(database))
    with patch("streamlit_autorefresh.st_autorefresh", return_value=0) as refresh:
        app = AppTest.from_file(str(APP), default_timeout=30).run()
        assert not app.exception
        assert app.title[0].value == "Resumen"
        assert len(app.get("plotly_chart")) == 3
        refresh.assert_called_with(interval=5_000, key="dashboard_refresh")
        app.multiselect(key="filter_symbols").set_value(["BTCUSDT"])
        app.selectbox(key="filter_mode").set_value("paper")
        app.date_input(key="filter_dates").set_value((date(2026, 9, 22), date(2026, 9, 22)))
        app.run()
        paths = ["1_Historial_Trades.py", "2_Señales_y_Rechazos.py", "3_Riesgo_y_Exposición.py", "4_Logs_del_Sistema.py"]
        for path in paths:
            app.switch_page(f"pages/{path}").run()
            assert not app.exception
            assert app.multiselect(key="filter_symbols").value == ["BTCUSDT"]
            assert app.selectbox(key="filter_mode").value == "paper"
            assert app.session_state["dashboard_filters"].symbols == ("BTCUSDT",)
            if path.startswith("1_"):
                frame = app.dataframe[0].value
                assert frame["PnL"].tolist() == [25]
                assert frame["Precio salida"].tolist() == [112.5]
                assert "datetime" in str(frame["Fecha"].dtype)
            if path.startswith("2_"):
                assert app.dataframe[0].value["Elegible"].tolist() == ["Sí", "Sí"]
        refresh.reset_mock()
        app.toggle(key="auto_refresh").set_value(False).run()
        refresh.assert_not_called()


def test_missing_database_shows_message_without_creating_it(tmp_path, monkeypatch):
    missing = tmp_path / "missing.db"
    monkeypatch.setenv("DASHBOARD_DB_PATH", str(missing))
    app = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not app.exception
    assert "No se encontró" in app.info[0].value
    assert not missing.exists()


def test_direct_link_bootstraps_entrypoint(database, monkeypatch):
    monkeypatch.setenv("DASHBOARD_DB_PATH", str(database))
    app = AppTest.from_file(str(APP), default_timeout=30)
    app.switch_page("pages/1_Historial_Trades.py").run()
    assert not app.exception
    assert app.title[0].value == "Historial de trades"
    assert app.multiselect(key="filter_symbols").value == []
    assert len(app.dataframe[0].value) == 3


def test_empty_database_and_partial_date_selection(database, tmp_path, monkeypatch):
    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    monkeypatch.setenv("DASHBOARD_DB_PATH", str(empty))
    app = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not app.exception
    for path in ["1_Historial_Trades.py", "2_Señales_y_Rechazos.py", "3_Riesgo_y_Exposición.py", "4_Logs_del_Sistema.py"]:
        app.switch_page(f"pages/{path}").run()
        assert not app.exception
        assert app.info
    app.date_input(key="filter_dates").set_value((date(2026, 9, 22),)).run()
    assert not app.exception
    assert any("Selecciona la fecha" in item.value for item in app.info)


def test_cached_reads_include_filters_expire_and_close_connections(database, selection):
    connections = []
    real_connect = dd.connect

    def tracked_connect(path):
        conn = real_connect(path)
        connections.append(conn)
        return conn

    with patch.object(dd, "connect", side_effect=tracked_connect) as connect:
        first = common.load_data(str(database), selection, "trades")
        assert common.load_data(str(database), selection, "trades") == first
        assert connect.call_count == 1
        common.load_data(str(database), dd.DashboardFilters(mode="live"), "trades")
        assert connect.call_count == 2
        time.sleep(4.1)
        common.load_data(str(database), selection, "trades")
        assert connect.call_count == 3
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")


def test_failed_read_closes_connection_and_ui_recovers(database, selection, monkeypatch):
    conn = dd.connect(str(database))
    with patch.object(dd, "connect", return_value=conn), patch.object(dd, "trade_history", side_effect=sqlite3.OperationalError("locked")):
        with pytest.raises(sqlite3.OperationalError):
            common.load_data(str(database), selection, "trades")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
    monkeypatch.setenv("DASHBOARD_DB_PATH", str(database))
    with patch.object(common, "load_options", side_effect=sqlite3.OperationalError("locked")):
        app = AppTest.from_file(str(APP), default_timeout=30).run()
        assert not app.exception
        assert app.warning
    app.run()
    assert not app.exception
    assert len(app.metric) == 4
