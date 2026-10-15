"""Closed trades: typed columns and native interactive sorting."""

import streamlit as st

from crypto_scalper.dashboard import common

common.require_entrypoint("1_Historial_Trades.py")
data = common.page_data("trades")
risk = data["risk"]
closed = risk["total_trades"]
with st.container(horizontal=True, gap="small"):
    st.metric("Trades cerrados", common.number(closed), border=True, width=220, height="stretch")
    st.metric("PnL realizado", f"{common.number(risk['realized_pnl'], 2, sign=True)} USDT" if closed else common.MISSING,
              delta=f"{common.number(risk['avg_pnl'], 2, sign=True)} USDT de media por trade" if closed else None,
              delta_color="normal" if closed and round(risk["avg_pnl"], 2) else "off",
              border=True, width=260, height="stretch", help="PnL bruto, antes de comisiones y financiación.")
    st.metric("Tasa de acierto", common.percent(risk["win_rate"], 1) if closed else common.MISSING,
              border=True, width=220, height="stretch")
common.table([
    {"Símbolo": r["symbol"], "Dirección": common.DIRECTIONS.get(r["side"], r["side"]),
     "Precio entrada": r["entry_price"], "Precio salida": r["exit_price"],
     "PnL": r["realized_pnl"], "Razón de cierre": common.reason(r["close_reason"]),
     "Fecha": common.timestamp(r["closed_ts_ms"])} for r in data["trades"]
], "trades_table", {"Precio entrada": 4, "Precio salida": 4, "PnL": 2})
st.caption("Fecha de cierre · PnL bruto antes de comisiones. El precio de salida procede de la orden "
           "ejecutada. Si no hay una orden de salida persistida, la celda queda sin valor (la tabla "
           "la muestra como «None»); no se deduce un precio a partir del PnL.")
