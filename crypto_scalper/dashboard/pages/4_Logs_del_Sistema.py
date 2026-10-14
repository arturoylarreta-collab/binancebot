"""Database audit log; filesystem logs and arbitrary payloads are never loaded."""

import streamlit as st

from crypto_scalper.dashboard import common

common.require_entrypoint("4_Logs_del_Sistema.py")
st.caption("Eventos, decisiones de riesgo y reconciliaciones persistidos en SQLite. "
           "Los archivos .log no forman parte de esta vista. Los payloads completos se omiten.")
data = common.page_data("logs")
common.table([
    {"Fecha": common.timestamp(r["ts_ms"]), "Origen": r["source"],
     "Símbolo": r["symbol"] or "Global / sin símbolo", "Detalle": common.reason(r["detail"])}
    for r in data["logs"]
], "logs_table")
