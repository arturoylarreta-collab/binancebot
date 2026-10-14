"""Signal audit: eligibility and downstream rejection remain distinct."""

import streamlit as st

from crypto_scalper.dashboard import charts, common

common.require_entrypoint("2_Señales_y_Rechazos.py")
data = common.page_data("signals")
charts.signal_chart(data["distribution"])
common.table([
    {"Símbolo": r["symbol"], "Score": r["score"],
     "Dirección": common.DIRECTIONS.get(r["signal_type"], r["signal_type"]),
     "Elegible": "Sí" if r["eligible"] else "No",
     "Motivo de rechazo": common.reason(r["rejection_reason"]),
     "Fecha": common.timestamp(r["ts_ms"])} for r in data["signals"]
], "signals_table", ("Score",))
st.caption("Una señal elegible puede ser rechazada posteriormente por riesgo o por el estado de la operativa.")
