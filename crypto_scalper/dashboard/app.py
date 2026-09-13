"""Crypto Scalper Observability Dashboard (FASE 8.5).

Capa fina de Streamlit sobre `crypto_scalper.monitoring.dashboard_data.py`.
Lee EXCLUSIVAMENTE la SQLite que escribe el bot (paper/backtest): sin estado de
cola, sin imports a los motores, sin red.  Path por env `DASHBOARD_DB_PATH`
(default `logs/paper.db`).

Arranque:

    streamlit run crypto_scalper/dashboard/app.py
"""

from __future__ import annotations

import os

import streamlit as st

from crypto_scalper.monitoring import dashboard_data as dd

st.set_page_config(page_title="Crypto Scalper", layout="wide")
st.title("Crypto Scalper - Observability")

db_path = os.environ.get("DASHBOARD_DB_PATH", "logs/paper.db")
if not os.path.exists(db_path):
    st.error(f"DB no encontrada: {db_path} (configura DASHBOARD_DB_PATH)")
    st.stop()

conn = dd.connect(db_path)

status = dd.bot_status(conn)
if "error" not in status:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Modo / Estado", f"{status['mode']} / {status['status']}")
    c2.metric("Equity", f"{status['equity']:,.2f} USDT")
    c3.metric("Drawdown", f"{status['drawdown_pct']*100:.2f}%")
    c4.metric("Posiciones abiertas", status["open_count"])
    c5.metric("Trades hoy", status["trades_today"])
    c6.metric("Uptime (h)", f"{status['uptime_ms']/3_600_000:.1f}")
else:
    st.info(f"Sin heartbeats aún ({status}). El bot debe correr una vez y persistir.")

st.subheader("PnL diario")
pnl = dd.daily_pnl(conn)
r, u, t, d = st.columns(4)
r.metric("Realized", f"{pnl['realized']:+,.2f}")
u.metric("Unrealized", f"{pnl['unrealized']:+,.2f}")
t.metric("Trades hoy", pnl["trades_today"])
d.metric("Drawdown", f"{pnl['drawdown_pct']*100:.2f}%")

st.subheader("Métricas de riesgo")
rm = dd.risk_metrics(conn)
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Trades cerrados", rm["total_trades"])
m2.metric("Win rate", f"{rm['win_rate']*100:.1f}%")
m3.metric("PnL medio", f"{rm['avg_pnl']:+.4f}")
m4.metric("Exposición total", f"{rm['total_exposure']:,.2f}")
m5.metric("Abiertas", rm["open_count"])
m6.metric("Max drawdown", f"{rm['max_drawdown_pct']*100:.2f}%")

st.subheader("Posiciones abiertas")
positions = dd.open_positions(conn)
if positions:
    st.dataframe(positions)
else:
    st.caption("Sin posiciones abiertas")

st.subheader("Últimas señales")
signals = dd.recent_signals(conn, limit=10)
if signals:
    st.dataframe(signals)
else:
    st.caption("Sin señales registradas todavia")