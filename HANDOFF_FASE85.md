# FASE 8.5 — Observabilidad (HANDOFF)

Capa de observación **solo-lectura/notificación** sobre el hub ya existente:
alertas Telegram asíncronas (fire-and-forget), heartbeat con equity/drawdown,
audit de señales con motivo de rechazo y dashboard Streamlit que lee
exclusivamente la SQLite. **No se toca el Risk Engine ni el Execution Engine.**

## Cómo lanzar

```powershell
# 1) env locales (gitignored): token/chat de tu bot + master switch
#    .env: TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... MONITORING_TELEGRAM_ENABLED=true
python -m crypto_scalper.main --mode paper --symbols BTCUSDT

# 2) dashboard (otro proceso; solo lectura de la DB)
streamlit run crypto_scalper/dashboard/app.py

# 3) suite completa offline
python -m pytest -q     # 392 tests (379 FASE 8 + 13 nuevos)
```

## Piezas nuevas

| Módulo | Rol |
|---|---|
| `monitoring/alerts.py` | `AlertEvent`, `format_message` (pura), `TelegramNotifier`: cola acotada + worker; `emit()` = `put_nowait` no bloqueante (drop si llena → métrica); `_post` seam HTTP (tests sin red) |
| `monitoring/observability.py` | `ObservabilityRepository` envuelve `Repository`: TRADE_OPENED/CLOSED por transición de estado (dedupe), RISK_HALT/KILL_SWITCH desde `RiskDecision` (cooldown 300 s), `pnl_unrealized` por mark `attach_mark`, heartbeat throttled |
| `config/monitoring.py` | `MonitoringConfig` + env `TELEGRAM_*/MONITORING_*/DASHBOARD_DB_PATH` |
| `monitoring/dashboard_data.py` | Agregaciones puras sqlite (`connect` readonly WAL, `bot_status`, `daily_pnl`, `open_positions`, `recent_signals`, `risk_metrics`); compat FASE 8 (colores ausentes → defaults) |
| `dashboard/app.py` | Streamlit fino: metrics + tablas; lee SOLO la DB |
| tests | `test_monitoring.py` (8) + `test_dashboard_data.py` (3) |

## Decisiones clave

1. **Alerta ≠ bloqueo**: el caminho crítico nunca hace `await` del HTTP. `emit()`
   mete texto en una `asyncio.Queue(maxsize=monitoring.queue_size)`; un worker
   único hace el POST a `api.telegram.org/bot<token>/sendMessage`. Cola llena →
   drop contado (`alerts.dropped_queue_full`), jamás backlog del executor.
2. **Observador, no motor**: el hook se injerta en la persistencia que el
   orquestador ya usa (construido en `main._build_observability_repository`), con
   hooks duck-typed `attach_mark`/`set_mode` en `paper/engine.py` y
   `backtest/engine.py`. `risk_engine.py` y `execution/*` quedan intactos.
3. **Sin mensajes duplicados**: las alertas de trade solo se disparan en
   transiciones `_OPEN_STATUSES → CLOSED` (o primer persistido abierto); los halts
   llevan cooldown por `event:reason:symbol`.
4. **Mark del adapter/venue**: `pnl_unrealized` se calcula con el precio actual del
   adapter (`paper`) o `BarVenue.get_price` (`backtest`); fallback al último
   `FeatureSnapshot.price`. Posiciones terminales persisten 0.0.
5. **Heartbeat = secuencia**: `equity = start_equity + realized + unrealized`,
   drawdown por pico de equity en memoria, exposición = notional de abiertas,
   `trades_today` por `closed_ts_ms` del día UTC. Throttled a
   `MONITORING_HEARTBEAT_INTERVAL_S` (default 5 s).
6. **Migraciones aditivas**: `sqlite_repo` añade `positions.pnl_unrealized`,
   tablas `signals` y `heartbeats` vía PRAGMA + `ALTER TABLE` (idempotente); las
   DB viejas de FASE 8 siguen leyéndose (dashboard tolera columnas/tablas
   ausentes).
7. **Audit de señales**: `TradeOrchestrator` persiste cada señal con
   `rejection_reason` (incluida las bloqueadas por pause/símbolo-abierto/FLAT);
   para rechazos del Risk se usa `decision.reason`/`verdict` cuando el verdict no
   es APPROVED (`outcome.error` es None en esos casos).

## Verificación

- `test_monitoring.py`: format de 5 eventos, notifier disabled, cola llena sin
  bloquear, seam `_post` (sin red), transición open→close con mark (1.0 persistido),
  dedupe de halt + KILL_SWITCH, heartbeat throttled, `save_signal`/`save_heartbeat`,
  mark-fallback desde snapshot.
- `test_dashboard_data.py`: repo→DB→dashboard (heartbeats/signals/posiciones),
  equity con mark a 110 → unrealizado +1.0, y esquema legacy FASE 8
  (sin `pnl_unrealized`/`signals`/`heartbeats`) degrada con defaults.
- `python -m pytest -q` → **392 passed** (7 warnings joblib/NumPy preexistentes).

## Limitaciones

- Telegram es best-effort: sin conectividad, timeout, o cola llena → drop con log;
  el bot nunca se detiene por una alerta.
- `pnl_unrealized` y equity del heartbeat son aproximaciones del simulador
  (sin funding/latencia real); los números finales siguen saliendo del
  `PaperAccount`/`BacktestReport`.
- El dashboard no re-numera (usa la última DB); `DASHBOARD_DB_PATH` debe apuntar a
  la DB de la sesión (paper default `logs/paper.db`).

## PROMPT PARA FASE 9 — LIVE adapter (siguiente sesión)

Cuando se autorice, el orden sugerido (nada de esto se ha ejecutado aún):
1. `execution/live.py`: `BinanceFuturesLiveAdapter(ExchangeAdapter)` con
   credenciales POR ENTORNO (`LIVE_TRADING_ENABLED=true` + API key/secret
   separadas), firma HMAC, recvWindow, retry/timeout del `OrderManager`.
2. `build_execution_stack("live", ...)`: venue real para submit/cancel/get_order
   y reconciliación de IDs; el resto del stack (PositionManager invariantes SL+TP,
   router, orquestador) ya es reutilizable tal cual.
3. Roles: `LIVE_TRADING_ENABLED=false` (default) bloquea el arranque.