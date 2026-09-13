# HANDOFF — FASE 7: Paper Trading (offline)

> Objetivo cumplido: ciclo completo `Señal → Riesgo → Ejecución (simulada)` en un bucle
> ejecutable sobre el feed de datos, con equity mark-to-market, persistencia SQLite
> auditable, reconciliación que mitiga la deriva y tope one-shot de trades. Sin romper
> FASE 1-6 (338 tests, todos verdes). **No hay dinero ni órdenes reales** (adapter LIVE
> sigue siendo `NotImplementedError`).

---

## Cómo lanzar

```powershell
python -m pytest -q        # 338 passed
# Tests nuevos de FASE 7 (44):
python -m pytest -q tests/test_paper_account.py tests/test_trading_orchestrator.py tests/test_trading_reconciler.py tests/test_storage_sqlite.py tests/test_paper_engine.py

# Sesión paper acotada sobre el feed real (sin dinero real):
python -m crypto_scalper.main --mode paper --symbols BTCUSDT --trades 5
```

La sesión deja el audit trail en `logs/paper.db` (`PAPER_DB_PATH`).

---

## Qué se añadió

```
crypto_scalper/
  config/paper.py               PaperConfig (start_equity, fee_pct, reconcile/summary interval, max_open_per_symbol, db_path)
  config/settings.py            Settings.risk (RISK_*) + Settings.paper (PAPER_*) wire a env; ya no hardcode
  storage/base.py               Repository: + save_position/save_order/save_fills/save_reconciliation/save_risk_decision/load_* (Noop impl)
  storage/sqlite_repo.py        SqliteRepository (audit trail idempotente: positions/orders/fills/risk_decisions/reconciliations/events)
  trading/account.py            PaperAccount (+PaperAccountStats): cash, realized PnL, fees, unrealized, peak equity/drawdown
  trading/orchestrator.py       TradeOrchestrator (+TradeSummary): on_signal/on_price, guard símbolo, FLAT gate, pausa Fatal, persist
  trading/reconciler.py         PeriodicReconciler: cadencia, mitigación (cierre/cancel/flatten), re-check, persist report, pausa
  paper/engine.py               PaperTradingEngine: source→señal→orquestador, summary cadence, max_trades one-shot
  execution/order_manager.py    + refresh() (sync caché con venue para persistencia fiel)
  execution/position_manager.py _finalize_close cancela el hermano restante de protección (higiene del book)
  main.py                       --mode paper, --trades N, run_paper(), _snapshot_source(), _feature_worker con bucle real
tests/test_paper_account.py     8 tests
tests/test_trading_orchestrator.py  13 tests
tests/test_trading_reconciler.py   7 tests
tests/test_storage_sqlite.py      12 tests
tests/test_paper_engine.py         4 tests
```

---

## Decisiones clave

1. **El orquestador es el gate de nivel aplicación** (además del Risk Engine):
   - nunca abre el mismo símbolo dos veces (`PAPER_MAX_OPEN_PER_SYMBOL`, por defecto 1);
   - una señal FLAT/neutra (`signal_type not in (LONG, SHORT)`) jamás llega al venue
     (el SignalEngine marca `eligible=True` con FLAT ~50; el gateway `flat_signal_not_eligible`
     lo bloquea — verificado en `test_neutral_signal_not_eligible`);
   - un error Fatal (`PositionNotProtectedError` / `ProtectionTimeoutError`) → `_pause()`
     y todo lo que sigue se bloquea con `orchestrator_paused:<motivo>`.

2. **Equity mark-to-market real**: `PaperAccount` con cash + realized PnL + fees
   (taker sobre notional de entrada y salida), y `unrealized_pnl()` calculado al precio
   vigente del venue; `build_portfolio()` inyecta esa equity al `PortfolioState` frozen
   que consume el Risk Engine. El sizing por riesgo (FASE 5) usa equity viva.

3. **Persistencia fiel y idempotente**: `SqliteRepository` upserta posiciones por
   `position_id`, órdenes por `client_order_id`, desduplica fills en proceso, y guarda
   decisiones de riesgo + reconciliaciones + eventos en un solo archivo. `OrderManager.refresh()`
   (**nuevo**) re-fetchea cada orden del venue antes de persistir: el histórico refleja
   el estado terminal verdadero (un TP llenado por tick dejaba la caché en `NEW`, lo cual
   era una mentira de auditoría). Noop sigue siendo el backend por defecto del repo.

4. **Reconciliación que mitiga, no solo reporta** (`PeriodicReconciler`):
   - `MISSING_STOP`/`MISSING_TP` → `pm.close_position(reason="reconcile_missing_protection")`
     (nunca operar desprotegido);
   - `ORPHAN_ORDER` → cancel;
   - `UNTRACKED_POSITION` → flatten con market reduce-only `FLAT-{symbol}-{uuid8}`;
   - tras mitigar, re-check: lo que sigue **Fatal** (POSITION_MISSING/QTY_MISMATCH/
     gap sin resolver) → `set_paused("reconciliation_fatal:...")` y pausa el orquestador.
   - Cada pase persistido (el reporte final post-mitigación).

5. **Higiene del book en cierre**: cuando SL/TP llena y cierra una posición, el hermano
   reduce-only que quedaba resting se cancela (verificado en el stream de eventos:
   `TP FILLED` seguido de `SL CANCELED`). Evita que un stop residual pivote el net en el
   siguiente movimiento de precio.

6. **Tope one-shot de trades**: `PaperTradingEngine.run(max_trades=N)` revisa el tope
   después de `on_price` y ANTES de `on_signal` (no abre un trade extra en el mismo tick
   que cierra el N-ésimo), y `persist()` corre incluso cuando se rompe, dejando el estado
   final auditable antes de cerrar el repo.

7. **`_feature_worker` arreglado**: FASE 2-6 computaba features UNA vez y salía; ahora
   es un `while not stop_event.is_set()` para alimentar la sesión paper.

---

## Verificación

- `python -m pytest -q` → **338 passed** (294 FASE 1-6 + 44 FASE 7).
- Tests nuevos de FASE 7: 44, todos deterministas (sin red):
  - `test_paper_account.py`: equity inicial, validaciones, long profit/short loss, fees
    por notional, mark-to-market unrealized, peak/drawdown, stats shape.
  - `test_trading_orchestrator.py`: APPROVED abre posición protegida; rechazado por riesgo
    (atr=0) sin tocar el venue; guard por símbolo; neutral nunca ejecuta; tick→TP ganancia,
    tick→SL pérdida, short TP, fees, equity al portfolio; Fatal → pausa + bloqueo posterior.
  - `test_trading_reconciler.py`: limpio; stop perdido→cierre `reconcile_missing_protection`;
    huérfana→cancel; no-trackeada→flatten; QTY_MISMATCH irresoluble→pausa; cadencia;
    reportes persistidos.
  - `test_storage_sqlite.py`: roundtrip de position/order+fills, idempotencia (upsert por
    position_id/client_order_id, dedupe de fills), risk decisions, reconciliaciones,
    snapshots/eventos, reopen de archivo, y trade real ruteado→persistido→recargado.
  - `test_paper_engine.py`: 2 trades deterministas con tope, fuente neutra sin trades,
    audit trail en archivo reabierto, stop event.

---

## Notas para FASE 8 (backtest / LIVE)

- Los pesos del Signal Score siguen **sin validar** (`config/strategies.py`):
  backtesting histórico es la FASE 8.
- El modelo de costes real (fees+spread+slippage+funding) queda para backtest; aquí las
  comisiones son un placeholder taker.
- Para LIVE: crear `binance_adapter.py` que implemente `ExchangeAdapter` con reconciliación
  de IDs, credenciales por entorno separado y `LIVE_TRADING_ENABLED=true` explícito; el
  orquestador/reconciler/persistencia de FASE 7 ya están cableados para reutilizarlos.
- Si la sesión paper crece, valorar pasar `events`/snapshots a un almacén aparte del
  SQLite de auditoría (los fills/órdenes/posiciones son lo canónico).