"""Plotly figures with Spanish labels and stable interaction state."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from crypto_scalper.dashboard.common import DIRECTIONS, MODE_LABELS, timestamp
from crypto_scalper.monitoring.dashboard_data import Row

GREEN = "#16a34a"
RED = "#dc2626"
BLUE = "#2563eb"
# Plotly's bundled JS only includes English; provide local translations instead
# of fetching a locale from a CDN during dashboard reads.
SPANISH_LOCALE = {
    "dictionary": {
        "Download plot as a PNG": "Descargar gráfico como PNG", "Zoom": "Ampliar",
        "Pan": "Desplazar", "Zoom in": "Acercar", "Zoom out": "Alejar",
        "Autoscale": "Ajustar escala", "Reset axes": "Restablecer ejes",
        "Fullscreen": "Pantalla completa", "Close": "Cerrar",
        "Double-click to zoom back out": "Haz doble clic para restablecer la vista",
        "Click to enter Colorscale title": "Pulsa para editar el título",
    },
    "format": {
        "days": ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"],
        "shortDays": ["dom", "lun", "mar", "mié", "jue", "vie", "sáb"],
        "months": ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"],
        "shortMonths": ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"],
        "date": "%d/%m/%Y", "decimal": ",", "thousands": ".",
    },
}


# Day-first tick labels at every zoom level; Plotly's default is "sep 22, 2026".
DATE_TICKS = [
    dict(dtickrange=[None, 1_000], value="%H:%M:%S.%L"),
    dict(dtickrange=[1_000, 60_000], value="%H:%M:%S<br>%d %b"),
    dict(dtickrange=[60_000, 86_400_000], value="%H:%M<br>%d %b"),
    dict(dtickrange=[86_400_000, "M1"], value="%d %b<br>%Y"),
    dict(dtickrange=["M1", "M12"], value="%b %Y"),
    dict(dtickrange=["M12", None], value="%Y"),
]


def date_axis(fig: go.Figure, title: str) -> None:
    fig.update_xaxes(title_text=title, tickformatstops=DATE_TICKS, hoverformat="%d/%m/%Y %H:%M:%S UTC")


def show(fig: go.Figure, key: str) -> None:
    """Retain zoom across refreshes; reset when the global selection changes.

    The legend grows upwards from the plot area and Plotly widens the top
    margin for wrapped rows, so in narrow windows it covers neither axis
    labels nor the toolbar that touch screens always show.
    """
    fig.update_layout(
        height=380, margin=dict(l=20, r=20, t=70, b=20),
        legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, xanchor="left"),
        uirevision=repr(st.session_state.get("dashboard_filters")),
        separators=",.", hovermode="x unified",
    )
    st.plotly_chart(fig, key=key, width="stretch", config={
        "displaylogo": False, "locale": "es", "locales": {"es": SPANISH_LOCALE},
        "modeBarButtonsToRemove": ["select2d", "lasso2d", "zoomIn2d", "zoomOut2d", "autoScale2d"],
    })


def equity_chart(rows: list[Row]) -> None:
    """Dual axes: capital in USDT and stored drawdown fraction as percent."""
    st.subheader("Capital y drawdown")
    if not rows:
        st.info("No hay heartbeats en este intervalo.")
        return
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for index, mode in enumerate(sorted({r["mode"] for r in rows})):
        values = [r for r in rows if r["mode"] == mode]
        dates = [timestamp(r["ts_ms"]) for r in values]
        name = MODE_LABELS.get(mode, mode)
        fig.add_trace(go.Scatter(x=dates, y=[r["equity"] for r in values], mode="lines+markers",
                                marker_size=3, name=f"Capital · {name}",
                                line=dict(color=[BLUE, GREEN, "#7c3aed"][index % 3]),
                                hovertemplate="%{y:,.2f} USDT<extra>%{fullData.name}</extra>"), secondary_y=False)
        fig.add_trace(go.Scatter(x=dates, y=[r["drawdown_pct"] * 100 for r in values],
                                mode="lines", name=f"Drawdown · {name}",
                                line=dict(color=RED, dash=["dot", "dash", "dashdot"][index % 3]),
                                hovertemplate="%{y:.2f}%<extra>%{fullData.name}</extra>"), secondary_y=True)
    date_axis(fig, "Fecha (UTC)")
    fig.update_yaxes(title_text="Capital (USDT)", secondary_y=False)
    # Explicit auto ticks: synced ticks produced labels such as 1,973 %.
    fig.update_yaxes(title_text="Drawdown (%)", rangemode="tozero", tickmode="auto",
                     showgrid=False, secondary_y=True)
    show(fig, "equity_chart")
    st.caption("Cartera completa · Hasta 2.001 puntos por modo, con primer y último registro. "
               "El muestreo puede omitir picos intermedios; el máximo de riesgo usa todos los registros.")


def pnl_chart(rows: list[Row]) -> None:
    """Plot the accumulated result after aggregation over the full interval."""
    st.subheader("PnL realizado acumulado")
    if not rows:
        st.info("No hay operaciones cerradas en este intervalo.")
        return
    fig = go.Figure(go.Scatter(
        x=[timestamp(r["ts_ms"]) for r in rows], y=[r["cumulative_pnl"] for r in rows],
        mode="lines+markers", marker_size=3,
        line=dict(color=GREEN if rows[-1]["cumulative_pnl"] >= 0 else RED),
        name="PnL acumulado", hovertemplate="%{y:+,.2f} USDT<extra>PnL acumulado</extra>",
    ))
    fig.add_hline(y=0, line_color="#94a3b8", line_dash="dot")
    date_axis(fig, "Fecha de cierre (UTC)")
    fig.update_layout(yaxis_title="PnL (USDT)", showlegend=False)
    show(fig, "pnl_chart")
    st.caption("Suma desde cero dentro del intervalo, por fecha de cierre. "
               "PnL bruto persistido en positions; no descuenta comisiones ni financiación.")


def signal_chart(rows: list[Row]) -> None:
    """Stack approved/rejected counts for LONG, SHORT and neutral signals."""
    st.subheader("Distribución de señales")
    if not rows:
        st.info("No hay señales para los filtros seleccionados.")
        return
    directions = sorted({r["signal_type"] for r in rows})
    total = sum(r["count"] for r in rows)
    fig = go.Figure()
    for approved, name, color in [(1, "Aprobadas*", GREEN), (0, "Rechazadas", RED)]:
        counts = [sum(r["count"] for r in rows if r["signal_type"] == d and r["approved"] == approved) for d in directions]
        fig.add_trace(go.Bar(x=[DIRECTIONS.get(d, d) for d in directions], y=counts, name=name,
                            marker_color=color, customdata=[100 * n / total for n in counts],
                            hovertemplate="%{x}: %{y} (%{customdata:.1f}% del total)<extra>%{fullData.name}</extra>"))
    fig.update_layout(barmode="stack", xaxis_title="Dirección", yaxis_title="Número de señales",
                      legend_traceorder="normal")
    tallest = max(sum(r["count"] for r in rows if r["signal_type"] == d) for d in directions)
    if tallest <= 10:  # Counts are whole numbers; avoid ticks such as 0,5.
        fig.update_yaxes(dtick=1)
    show(fig, "signal_chart")
    st.caption("*Aprobadas: LONG/SHORT elegibles sin rechazo registrado. No implica una orden ejecutada. "
               "Se incluyen señales neutras y rechazos posteriores a la elegibilidad.")


def exposure_chart(rows: list[Row]) -> None:
    """Show gross open notional, grouped by symbol and position direction."""
    st.subheader("Exposición por símbolo")
    if not rows:
        st.info("No hay posiciones abiertas para los filtros seleccionados.")
        return
    fig = go.Figure()
    for side, color in [("BUY", BLUE), ("SELL", "#d97706")]:
        values = [r for r in rows if r["side"] == side]
        if not values:
            continue
        fig.add_trace(go.Bar(x=[r["symbol"] for r in values], y=[r["exposure"] for r in values],
                            name=DIRECTIONS[side], marker_color=color,
                            hovertemplate="%{x}: %{y:,.2f} USDT<extra>%{fullData.name}</extra>"))
    # Keep the LONG/SHORT legend even when only one direction is open.
    fig.update_layout(barmode="stack", xaxis_title="Símbolo", yaxis_title="Nominal bruto (USDT)",
                      showlegend=True)
    show(fig, "exposure_chart")
