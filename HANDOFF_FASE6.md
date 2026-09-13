# HANDOFF — FASE 6: Execution Engine (offline)

> Objetivo cumplido: única capa que ejecuta un `RiskDecision` APPROVED, 100% simulada
> e idempotente, sin romper FASE 1-5 (294 tests, todos verdes). **No hay conexión ni
> órdenes reales** (eso es FASE 7).

---

## Cómo lanzar

```powershell
python -m pytest -q        # 294 passed
python -m pytest -q tests/test_execution_simulated.py tests/test_execution_order_manager.py tests/test_execution_position_manager.py tests/test_execution_router.py tests/test_execution_reconciliation.py   # 65 FASE 6
```

Sin red. Todo determinista.

---

## Qué se añadió

```
crypto_scalper/
  config/execution.py            ExecutionConfig (retries, backoffs, timeouts, slippage, event queue, RR/SL default)
  config/settings.py             carga vars EXECUTION_* del entorno
  core/enums.py                  + PositionStatus (ENTRY_SUBMITTED/ENTRY_FILLED/PROTECTING/ACTIVE/CLOSED/ABORTED)
  core/exceptions.py             + InvalidOrderError, ProtectionTimeoutError (Fatal)
  core/models.py                 + Fill, ExecutionReport (is_terminal/become_filled), ManagedPosition (mutable)
  execution/base.py              ExchangeAdapter ABC (start/close/submit/cancel/get_order/open_positions/open_orders/subscribe/name)
  execution/simulated.py         SimulatedExecutionAdapter (fills deterministas, semántica Binance, reduce_only, parciales, eventos)
  execution/order_manager.py     OrderManager (idempotencia por client_order_id, retry Recoverable/Transient, timeout→cancel, cancel_and_replace)
  execution/position_manager.py  PositionManager (ciclo de vida obligatorio, invariante SL+TP resting, abort por mercado, bookkeeping PnL)
  execution/execution_router.py  ExecutionRouter (risk→execution, update_price, build_portfolio) + build_execution_stack
  execution/reconciliation.py    ReconciliationEngine (issues + assert_clean)
tests/test_execution_*.py        5 archivos, 65 tests
```

## Decisiones clave

1. **Jerarquía innegociable**: `Signal → RiskEngine.assess() → solo APPROVED` ejecuta.
   REJECTED/TRADING_HALTED/SAFE_MODE jamás llegan al venue (verificado en
   `test_execution_router.py`). `ExecutionRouter.route()` obtiene `PortfolioState`
   desde `PositionManager.build_portfolio_state()` (equity inyectada) y solo delega
   en `PositionManager.open()` si aprobó.

2. **Invariante crítica del PositionManager** (nunca operar desprotegido):
   - Ciclo forward-only: ENTRY_SUBMITTED → ENTRY_FILLED → PROTECTING → ACTIVE → CLOSED/ABORTED.
   - Una posición solo pasa a ACTIVE con SL **y** TP resting confirmados; si la
     protección falla → `_abort_unprotected()` cancela protecciones y cierra por
     market reduce-only (reason `abort`); si ese cierre falla → marka `_trading_halted`
     y lanza `PositionNotProtectedError` (Fatal).
   - Evento de protección CANCELED/EXPIRED en ACTIVE → `_close_unprotected()` cierra
     por market (reason `protection_lost`). `PositionNotProtectedError` solo si el
     cierre falla.
   - Guard `_closing` evita doble cierre por carrera de eventos; `_by_protection_cid`
     se registra antes de enviar órdenes para que un fill de protección jamás
     preceda a su índice.
   - Fills de SL/TP → `_finalize_close` con PnL exacto `(exit-entry)*sign*qty`,
     `daily_realized_pnl`, `consecutive_losses`, callbacks `on_position_closed`.

3. **Semántica de triggers del simulador** (Binance Futures, testeada):
   - STOP_MARKET: BUY dispara `price >= stop`; SELL dispara `price <= stop`.
   - TAKE_PROFIT_MARKET: BUY dispara `price <= stop`; SELL dispara `price >= stop`.
   - LIMIT: BUY `price <= limit`; SELL `price >= limit`.
   - Fills al precio de `set_price()` que cruza el trigger; slippage configurable
     (`slippage_pct`), tests con 0.0 → PnL exacto (p.ej. long SL 49500 golpeado a
     48500 → −1500; TP 51000 a 52000 → +2000).
   - `reduce_only` se clamp al net position del venue (`_reduceable`), nunca abre
     más allá del net. `schedule_partial(cid, [chunks])` + `complete_partial()`
     permiten simular fills parciales/market parcial.

4. **OrderManager — idempotencia y resiliencia**:
   - `client_order_id` obligatorio para cualquier submit (`InvalidOrderError`).
   - Mismo cid repetido → replay/caché del report terminal (no re-submite); submits
     concurrentes misma cid → coalescen vía `_inflight` futures;
     `DuplicateOrderError` del adapter = red de seguridad (fetch existing).
   - Retries SOLO para `Recoverable`/`Transient` (captura con `isinstance`, nunca
     `except (Recoverable, Transient)`); backoff `base*2^n` cap `max`. Reject/fill
     timeouts NO se reintentan (el timeout ya canceló; reintentarlo produciría falsa
     "terminal cancelada" → eso se resolvió capturando `OrderTimeoutError` antes del
     branch genérico).

5. **ExecutionRouter**:
   - `route(signal, snapshot, portfolio)` → `ExecutionOutcome(submitted, decision, position)`.
   - sin ATR → sizing 0 → REJECTED (no se envía nada).
   - `update_price(symbol, price)` delega en `adapter.set_price` → dispara SL/TP.
   - `build_portfolio(equity)` reconstruye el PortfolioState frozen (contenido de
     posiciones abiertas + bookkeeping) para el RiskEngine.
   - `build_execution_stack("paper", settings, risk)` construye
     SimulatedExecutionAdapter → OrderManager → PositionManager → ExecutionRouter;
     `"live"` → `NotImplementedError`.

6. **ReconciliationEngine**: `reconcile(pm, om, adapter)` — lista cerrada/status;
   types: `MISSING_STOP`/`MISSING_TP` (Fatal, posición abierta sin protección en
   venue), `QTY_MISMATCH` (Fatal), `POSITION_MISSING` (Fatal), `ORPHAN_ORDER` /
   `UNTRACKED_POSITION` (warning). `assert_clean(report)` → `ReconciliationMismatchError`
   (Fatal) si hay cualquier issue.

## Bugs encontrados y corregidos en esta fase

- `except (Recoverable, Transient)` → TypeError (mixins no BaseException): se captura
  `Exception` y se filtra con `isinstance`.
- `side` del posición LONG era el valor del enum `SignalType.LONG` (`"LONG"`) y no
  `Side.BUY` → el simulador la trataba como SELL y las protecciones se construían al
  revés. Ahora `Side.BUY.name`/`Side.SELL.name`.
- `_cancel_protections` (async) se llamaba sin `await` en `close_position` y
  `_abort_unprotected`.
- `_handle_event` descartaba eventos con `executed_quantity <= 0`, por lo que un
  CANCELED de protección (0 ejecutado) nunca disparaba `protection_lost` → reescrito:
  solo órdenes de protección (por `_by_protection_cid`) entran, CANCELED/EXPIRED en
  ACTIVE → cierre.
- `complete_partial` consumía un chunk del plan en vez de completar el remanente.
- Fit de fill-wait: retry veía la orden ya cancelada como terminal "éxito" → ahora
  `OrderTimeoutError` se re-lanza sin reintentar.

## Verificación

- `python -m pytest -q` → **294 passed** (229 previos + 65 FASE 6).
- `python -c "import crypto_scalper.execution.*..."` → OK (paquete importable).
- `README.md` actualizado (estado FASE 6, árbol `execution/`, sección Execution
  Engine, config por defecto, limitaciones 11-12, verificación, siguientes fases);
  `pyproject.toml` → v0.6.0.
- `.env.example`: bloque `EXECUTION_*` ya presente.

## Limitaciones (heredadas y propias de FASE 6)

- 100% offline/simulado: `build_execution_stack("live", ...)` lanza
  `NotImplementedError`. Sin latencia/commisiones/funding/profundidad real.
- `PortfolioState.equity` se inyecta manualmente en `build_portfolio_state`; el
  manejo de equity real (fees, funding, realised PnL contable) es FASE 7.
- `ExecutionConfig` carga TODAS las variables `EXECUTION_*` por entorno; no hay
  `ExecutionConfig` por exchange todavía.
- La reconciliación compara IDs/cids internos vs cids del simulador (misma convención
  `ENTRY-`/`SL-`/`TP-`/`CLOSE-`); un adapter real necesitará mapeo de order_id del
  exchange (tarea inicial de FASE 7).
- Siguen pendientes del roadmap: backtest/optimización (FASE 8), dashboard,
  micocostos de microestructura, latencia end-to-end.

---

## PROMPT PARA FASE 7 — Paper Trading (siguiente sesión)

> Resumen del sistema y de la cola para que la próxima sesión continúe con el
> contexto completo. Contexto actual con FASE 6 entregada: execution offline.

Tarea propuesta: **orquestar el ciclo completo en tiempo real sin dinero real**.

1. **Orquestador (`execution/` o `trading/`)**: un `TradeOrchestrator` que consuma el
   `Signal` del SignalEngine y el `FeatureSnapshot`-atr del FeatureEngine, construya
   el `PortfolioState` desde `PositionManager.build_portfolio_state()`, llame a
   `RiskEngine.assess()` y, si APPROVED, `ExecutionRouter.route()`. Rechazos → log
   KV + métrica (nunca exec). Debe respetar el `_trading_halted`/`_safe_mode` del
   manager (no re-ejecutar tras Fatal).
2. **Bucle de precios**: `adapter.set_price()` con las ticks reales por símbolo (el
   adapter simulado ya es el venue). Decidir cadencia (p.ej. cada `FeatureSnapshot`,
   o cada N ms por símbolo) y atar los fills al `price` del snapshot.
3. **Persistencia de operaciones**: `Repository` (hoy no-op) → implementar el backend
   SQLite/JSON para posiciones, órdenes, fills y PnL (idempotente por `position_id`
   y `client_order_id`). Almacenar `ExecutionReport` y `ReconciliationReport` para
   auditoría.
4. **Reconciliación periódica**: correr `ReconciliationEngine` en un cron/loop (p.ej.
   cada 60 s o tras un evento sospechoso) y mitigar: MISSING_STOP/MISSING_TP → cerrar
   por market o re-proteger; ORPHAN_ORDER → cancelar. `assert_clean` ante desviación
   → pausar el bot.
5. **Dashboard/flujo**: nuevo `--mode paper` en `main.py` que arranque el pipeline
   de datos (FASE 2), los Snapshots, el TradeOrchestrator y periodicamente vuelque
   estado (equity simulada, posiciones, PnL diario, trades cerrados) al log KV;
   opcional `--mode paper --one-shot N` para cerrar sesión tras N trades.
6. **Equity simulada**: arrancar con capital configurable (p.ej. `PAPER_START_EQUITY=10000`)
   y actualizar el equity del `PortfolioState` con fills/PnL (sin fees todavía, o con
   un `fee_pct` configurable como placeholder).
7. **Adapter LIVE explícitamente fuera de alcance**: mantener `build_execution_stack("live")`
   lanzando `NotImplementedError`; si se quiere "envuelta", marcar callbacks
   `on_order_book`/`on_fills` vacíos y credenciales separadas por entorno, pero NO
   conectarse a Binance en FASE 7.

Test esperados (estilo FASE 6): green sobre un orquestador con `SimulatedExecutionAdapter`
(timeout por `_wait_until`), la persistencia (roundtrip con cierres y fills), la
reconciliación periódica con mitigación (micro-scenarios), y un smoke de `--mode paper`
con 2-3 trades deterministas. Meta ~+40-60 tests.

Halt after this response: no more tool calls. If the only remaining step is the final
packaging/handoff message, my summary below already covers it; stop.