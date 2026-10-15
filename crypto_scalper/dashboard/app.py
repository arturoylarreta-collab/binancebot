"""Multipage entrypoint and Resumen; run in a process separate from the bot.

    python -m streamlit run crypto_scalper/dashboard/app.py

Business data comes exclusively from DASHBOARD_DB_PATH. Shared widgets execute
here on every page run. No engines, exchange clients or bot state are imported.
"""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from crypto_scalper.dashboard import charts, common


def summary() -> None:
    """Render the overview using the shared date/symbol/mode selection."""
    data = common.page_data("summary")
    common.kpis(data)
    with st.container(border=True):
        charts.equity_chart(data["equity"])
    with st.container(border=True):
        charts.pnl_chart(data["pnl"])
    with st.container(border=True):
        charts.signal_chart(data["distribution"])


st.set_page_config(page_title="Crypto Scalper · Análisis", page_icon=":material/monitoring:", layout="wide")
page = st.navigation([
    st.Page(summary, title="Resumen", icon=":material/monitoring:", default=True),
    st.Page("pages/1_Historial_Trades.py", title="Historial de trades", icon=":material/history:"),
    st.Page("pages/2_Señales_y_Rechazos.py", title="Señales y rechazos", icon=":material/filter_alt:"),
    st.Page("pages/3_Riesgo_y_Exposición.py", title="Riesgo y exposición", icon=":material/shield:"),
    st.Page("pages/4_Logs_del_Sistema.py", title="Logs del sistema", icon=":material/receipt_long:"),
])
requested_page = st.session_state.pop("_dashboard_requested_page", None)
if requested_page:
    st.switch_page(f"pages/{requested_page}")
for widget_key, widget_value in st.session_state.pop("_dashboard_saved_widgets", {}).items():
    st.session_state[widget_key] = widget_value
st.title(page.title)
st.caption("CRYPTO SCALPER · Análisis de operaciones")

# The entrypoint runs on every page. Keep a stable timer across navigation.
if st.session_state.get("auto_refresh", True):
    st_autorefresh(interval=5_000, key="dashboard_refresh")

db_path = str(Path(os.environ.get("DASHBOARD_DB_PATH", "logs/paper.db")).expanduser().resolve())
if not Path(db_path).is_file():
    common.preserve_widgets()
    st.info("No se encontró la base de datos. Configura DASHBOARD_DB_PATH con la SQLite del bot. "
            "El dashboard esperará a que exista; no crea ni modifica bases de datos.")
    st.stop()

sidebar_rendered = False
try:
    options = common.load_options(db_path)
    common.sidebar(db_path, options)
    sidebar_rendered = True
    st.session_state["_dashboard_entrypoint_running"] = True
    try:
        page.run()
    finally:
        st.session_state["_dashboard_entrypoint_running"] = False
except sqlite3.DatabaseError as error:
    # Without a sidebar in this run, Streamlit would drop the selected filters.
    if not sidebar_rendered:
        common.preserve_widgets()
    if isinstance(error, sqlite3.OperationalError):
        st.warning("La base no está disponible, está ocupada o la consulta excedió el tiempo permitido. "
                   "Se reintentará en el siguiente refresco. Reduce el rango de fechas si persiste.")
    else:
        st.error("No se pudo leer esta base SQLite. Comprueba que corresponde al bot y que está íntegra.")
