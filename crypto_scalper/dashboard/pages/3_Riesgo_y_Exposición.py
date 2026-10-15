"""Persisted risk analysis; never imports or calls the RiskEngine."""

import streamlit as st

from crypto_scalper.dashboard import charts, common

common.require_entrypoint("3_Riesgo_y_Exposición.py")
data = common.page_data("risk")
common.kpis(data)  # The drawdown card includes the period maximum.
st.caption("Exposición actual: posiciones todavía abiertas y con fecha de apertura dentro del filtro. "
           "El esquema conserva su último estado, por lo que no reconstruye posiciones abiertas en una fecha pasada.")
charts.exposure_chart(data["exposure"])
st.subheader("Posiciones abiertas")
common.table([
    {"Símbolo": r["symbol"], "Dirección": common.DIRECTIONS.get(r["side"], r["side"]),
     "Cantidad": r["quantity"], "Precio entrada": r["entry_price"],
     "Exposición (USDT)": abs(r["notional_value"]), "Límite de pérdida": r["stop_loss_price"],
     "Objetivo de beneficio": r["take_profit_price"], "PnL no realizado": r["pnl_unrealized"],
     "Fecha": common.timestamp(r["opened_ts_ms"])} for r in data["positions"]
], "positions_table", {"Cantidad": 4, "Precio entrada": 4, "Exposición (USDT)": 2,
                         "Límite de pérdida": 4, "Objetivo de beneficio": 4, "PnL no realizado": 2})
