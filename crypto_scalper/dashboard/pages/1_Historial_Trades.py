"""Closed trades: typed columns and native interactive sorting."""

import streamlit as st

from crypto_scalper.dashboard import common

common.require_entrypoint("1_Historial_Trades.py")
data = common.page_data("trades")
risk = data["risk"]
with st.container(horizontal=True):
    st.metric("Trades cerrados", risk["total_trades"], border=True)
    st.metric("PnL realizado", f"{risk['realized_pnl']:+,.2f} USDT",
              delta=f"{risk['realized_pnl']:+,.2f} USDT", border=True)
    st.metric("Tasa de acierto", f"{risk['win_rate'] * 100:.1f}%" if risk["total_trades"] else "—", border=True)
common.table([
    {"Símbolo": r["symbol"], "Dirección": common.DIRECTIONS.get(r["side"], r["side"]),
     "Precio entrada": r["entry_price"], "Precio salida": r["exit_price"],
     "PnL": r["realized_pnl"], "Razón de cierre": common.reason(r["close_reason"]),
     "Fecha": common.timestamp(r["closed_ts_ms"])} for r in data["trades"]
], "trades_table", ("Precio entrada", "Precio salida", "PnL"))
st.caption("Fecha de cierre · PnL bruto antes de comisiones. El precio de salida procede "
           "de la orden ejecutada; queda vacío si no hay una orden de salida persistida.")
