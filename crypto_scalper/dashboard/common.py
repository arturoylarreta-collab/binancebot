"""Shared cached reads, UTC filters and Spanish presentation helpers."""

from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Literal

import streamlit as st

from crypto_scalper.monitoring import dashboard_data as dd

View = Literal["summary", "trades", "signals", "risk", "logs"]
MODE_LABELS = {None: "Todos", "backtest": "Backtest", "paper": "Simulación (paper)", "live": "En vivo (live)"}
DIRECTIONS = {"BUY": "LONG", "SELL": "SHORT", "FLAT": "NEUTRA"}
REASONS = {
    "take_profit": "Objetivo de beneficio", "tp": "Objetivo de beneficio",
    "stop_loss": "Límite de pérdida", "sl": "Límite de pérdida",
    "manual": "Cierre manual", "abort": "Operación abortada",
    "protection_lost": "Protección perdida", "shutdown": "Apagado del bot",
    "risk_rejected": "Rechazada por riesgo", "flat_signal_not_eligible": "Señal neutra",
    "symbol_already_open": "Símbolo con posición abierta",
    "orchestrator_paused": "Operativa pausada", "end_of_backtest": "Fin del backtest",
    "DAILY_LOSS_LIMIT": "Límite diario de pérdidas", "MAX_DRAWDOWN": "Caída máxima",
    "KILL_SWITCH": "Parada de emergencia", "LOW_CONFIDENCE": "Confianza insuficiente",
    "APPROVED": "Aprobada", "REJECTED": "Rechazada", "SAFE_MODE": "Modo seguro",
    "TRADING_HALTED": "Operativa detenida", "feature_snapshots": "Registro de indicadores",
}


@st.cache_data(ttl=4, max_entries=8, show_spinner=False)
def load_options(db_path: str) -> dd.Row:
    """Cache metadata by absolute path; never share a SQLite connection."""
    with closing(dd.connect(db_path)) as conn:
        return dd.filter_options(conn)


@st.cache_data(ttl=4, max_entries=32, show_spinner=False)
def load_data(db_path: str, filters: dd.DashboardFilters, view: View, limit: int = 1_000) -> dd.Row:
    """Cache by path, ALL filters, page and row cap for four seconds.

    Connections close even on query errors. Reads use individual short SQLite
    snapshots rather than holding a transaction open across an entire page.
    """
    with closing(dd.connect(db_path)) as conn:
        data: dd.Row = {}
        if view in ("summary", "risk"):
            data["status"] = dd.bot_status(conn, filters)
            data["risk"] = dd.risk_metrics(conn, filters)
            data["equity"] = dd.equity_history(conn, filters)
        if view == "summary":
            data["pnl"] = dd.cumulative_pnl(conn, filters)
            data["distribution"] = dd.signal_distribution(conn, filters)
        elif view == "trades":
            data["trades"] = dd.trade_history(conn, filters, limit)
            data["risk"] = dd.risk_metrics(conn, filters)
        elif view == "signals":
            data["signals"] = dd.recent_signals(conn, limit, filters)
            data["distribution"] = dd.signal_distribution(conn, filters)
        elif view == "risk":
            data["positions"] = dd.open_positions(conn, filters, limit)
            data["exposure"] = dd.exposure_by_symbol(conn, filters)
        elif view == "logs":
            data["logs"] = dd.system_logs(conn, filters, limit)
    data["read_at"] = datetime.now(timezone.utc)
    return data


def utc_date(ts_ms: int) -> date:
    """Convert persisted milliseconds to a UTC calendar date."""
    return datetime.fromtimestamp(ts_ms / 1_000, timezone.utc).date()


def date_filters(start: date, end: date, symbols: list[str], mode: str | None) -> dd.DashboardFilters:
    """Include the entire last date, independently of the server timezone."""
    return dd.DashboardFilters(
        start_ms=int(datetime.combine(start, time.min, timezone.utc).timestamp() * 1_000),
        end_ms=int(datetime.combine(end + timedelta(days=1), time.min, timezone.utc).timestamp() * 1_000),
        symbols=tuple(sorted(symbols)), mode=mode,
    )


def sidebar(db_path: str, options: dd.Row) -> dd.DashboardFilters:
    """Render in the entrypoint, preserving widget state across navigation."""
    today = datetime.now(timezone.utc).date()
    first = utc_date(options["min_ts_ms"]) if options["min_ts_ms"] else today
    last = max(today, utc_date(options["max_ts_ms"])) if options["max_ts_ms"] else today
    with st.sidebar:
        st.header("Filtros globales")
        dates = st.date_input("Rango de fechas (UTC)", value=(first, last), key="filter_dates", format="DD/MM/YYYY")
        symbols = st.multiselect("Símbolos", options["symbols"], key="filter_symbols", placeholder="Todos los símbolos")
        mode = st.selectbox("Modo", [None, *dd.MODES], format_func=lambda m: MODE_LABELS[m], key="filter_mode")
        st.selectbox("Filas por tabla", [100, 500, 1_000, 5_000], index=2, key="table_limit")
        st.toggle("Actualizar cada 5 segundos", value=True, key="auto_refresh")
        st.caption("Desactiva el refresco para explorar las tablas con calma. Los filtros se conservan al navegar.")
    if len(dates) != 2:
        st.info("Selecciona la fecha inicial y la final para consultar los datos.")
        st.stop()
    filters = date_filters(dates[0], dates[1], symbols, mode)
    st.session_state["dashboard_path"] = db_path
    st.session_state["dashboard_filters"] = filters
    if mode is not None and options["modes"] != (mode,):
        st.warning("Este modo no identifica de forma inequívoca las filas sin modo de esta base. "
                   "Esas tablas se muestran vacías; elige «Todos» o utiliza una base separada por modo.")
    if symbols:
        st.info("El filtro de símbolo se aplica a operaciones, señales, exposición y eventos. "
                "Capital y drawdown describen toda la cartera: SQLite no los desglosa por símbolo.")
    return filters


def page_data(view: View) -> dd.Row:
    """Read only the active page using context initialized by app.py."""
    data = load_data(st.session_state["dashboard_path"], st.session_state["dashboard_filters"],
                     view, st.session_state["table_limit"])
    st.caption(f"Lectura: {data['read_at']:%d/%m/%Y %H:%M:%S} UTC · Datos persistidos en SQLite")
    return data


def require_entrypoint(filename: str) -> None:
    """Bootstrap direct links discovered by Streamlit's legacy pages/ loader.

    On the first request of a fresh server, legacy discovery may run a page
    before app.py has registered st.navigation. Redirect through app.py and
    restore the intended page so all reads always use initialized filters.
    """
    if not st.session_state.get("_dashboard_entrypoint_running", False):
        # A redirect ends a run without rendering its widgets. Save their
        # values under a non-widget key before Streamlit cleans them up.
        st.session_state["_dashboard_saved_widgets"] = {
            key: st.session_state[key]
            for key in ("filter_dates", "filter_symbols", "filter_mode", "table_limit", "auto_refresh")
            if key in st.session_state
        }
        st.session_state["_dashboard_requested_page"] = filename
        st.switch_page("app.py")


def timestamp(ts_ms: int) -> datetime | None:
    """Keep dates typed for sorting; display absent timestamps as null."""
    return datetime.fromtimestamp(ts_ms / 1_000, timezone.utc) if ts_ms else None


def reason(value: str | None) -> str:
    """Translate known reasons and preserve unknown audit codes verbatim."""
    if not value:
        return "—"
    code, sep, detail = value.partition(":")
    return REASONS.get(code, code) + (f": {REASONS.get(detail.strip(), detail.strip())}" if sep else "")


def table(rows: list[dd.Row], key: str, numeric: tuple[str, ...] = ()) -> None:
    """Sortable, read-only dataframe with typed numeric/date cells and stable key."""
    if not rows:
        st.info("No hay registros para los filtros seleccionados.")
        return
    config: dict[str, Any] = {column: st.column_config.NumberColumn(format="%.4f") for column in numeric}
    config["Fecha"] = st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm:ss", timezone="UTC")
    st.dataframe(rows, hide_index=True, width="stretch", column_config=config, key=key)
    limit = st.session_state["table_limit"]
    st.caption(f"{len(rows):,} filas mostradas · Máximo: {limit:,}, las más recientes. "
               "Pulsa cualquier encabezado para ordenar las filas cargadas. Fechas en UTC.")


def kpis(data: dd.Row) -> None:
    """Portfolio capital/drawdown and filtered win rate/current open exposure."""
    status, risk, history = data["status"], data["risk"], data["equity"]
    valid = "error" not in status
    same_mode = [r for r in history if r["mode"] == status.get("mode")]
    change = status["equity"] - same_mode[0]["equity"] if valid and same_mode else None
    capital, drawdown, win_rate, exposure = st.columns(4)
    with capital:
        st.metric("Capital (USDT)", f"{status['equity']:,.2f}" if valid else "—",
                  delta=f"{change:+,.2f} USDT" if change is not None else None,
                  border=True, help="Último heartbeat de cartera. Cambio frente al primero del mismo modo en el intervalo.")
    with drawdown:
        st.metric("Drawdown", f"{status['drawdown_pct'] * 100:.2f}%" if valid else "—",
                  delta=f"{-status['drawdown_pct'] * 100:.2f}% desde el máximo" if valid else None,
                  border=True, help="Caída de toda la cartera guardada por el bot; no se recalcula por símbolo.")
    with win_rate:
        st.metric("Tasa de acierto", f"{risk['win_rate'] * 100:.1f}%" if risk["total_trades"] else "—",
                  delta=f"{risk['realized_pnl']:+,.2f} USDT" if risk["total_trades"] else None,
                  border=True, help="Trades con PnL positivo / trades cerrados del filtro. El indicador muestra su PnL total.")
    with exposure:
        st.metric("Exposición (USDT)", f"{risk['total_exposure']:,.2f}", border=True,
                  help="Nominal bruto de posiciones actualmente abiertas cuya apertura cumple los filtros; no es exposición histórica.")
    if valid:
        state = {"running": "En ejecución", "stopped": "Detenido", "paused": "Pausado"}.get(status["status"], status["status"])
        st.caption(f"Último heartbeat del intervalo: {timestamp(status['ts_ms']):%d/%m/%Y %H:%M:%S} UTC · "
                   f"{MODE_LABELS.get(status['mode'], status['mode'])} · Estado registrado: {state}")
    else:
        st.info(status["error"])
