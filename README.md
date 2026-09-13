# crypto_scalper — quantitative, async, modular crypto scalping system

Status: **FASE 1 (arquitectura/riesgo) + FASE 2 (ingesta de datos) + FASE 3 (motor de features y señales) + FASE 4 (filtro ML) + FASE 5 (Risk Engine) + FASE 6 (Execution Engine offline) + FASE 7 (Paper Trading offline) implementadas.**

FASE 2-4 producen un `FeatureSnapshot` estructurado por activo con features por
categoría, un `Signal` Score 0-100 con desglose por dimensión y una dimensión
`ml` alimentada por un clasificador probabilístico ensayado offline con walk-forward
validation. La **FASE 5 añade el Risk Engine**: autoridad final sobre cada Signal
(aprueba o rechaza por límites duros de riesgo, position sizing por riesgo y SL/TP
obligatorios), sin red y determinista. La **FASE 6 añade el Execution Engine offline**:
única capa que ejecuta un `RiskDecision` APPROVED con `ExchangeAdapter` +
`SimulatedExecutionAdapter`, `OrderManager`, `PositionManager` (secuencia obligatoria
Entry → Fill → Protección → Active, nunca activa sin SL+TP resting), `ExecutionRouter`
(el riesgo manda: solo APPROVED llega al venus) y `ReconciliationEngine`. La **FASE 7
añade el Paper Trading offline**: `TradeOrchestrator` (ciclo señal→riesgo→ejecución con
equity mark-to-market de `PaperAccount`, guard por símbolo y pausa ante errores Fatales),
persistencia SQLite idempotente del audit trail (`SqliteRepository`), `PeriodicReconciler`
que mitiga la deriva venue-interno (cierra posiciones desprotegidas, cancela huérfanas,
aplana no-trackeadas) y `PaperTradingEngine` + `--mode paper` para simular sesiones.
Es 100% simulado e idempotente: **no hay conexión real ni órdenes reales** (eso es una
fase posterior con un adapter LIVE).

---

## Qué hay implementado

```
crypto_scalper/
  config/            settings (env externo), risk (límites duros), symbols, strategies
  core/              models, events (EventBus), enums, exceptions (jerarquía tipada)
  market_data/       rest (aiohttp, weight-aware), websocket (combined stream,
                     reconnect backoff+jitter, ping/pong), orderbook (Sync L2 U/u/pu),
                     trades (agregación por ventanas 1s-5m), candles (1s),
                     universe (Tradability Score + filtros), state, processor
  features/          technicals (EMA/RSI/ATR/ADX/VWAP/BB/ROC, NumPy), price/volume/
                     volatility, orderflow, categories (10 categorías), feature_engine, regime
  strategies/        signal_engine (Signal Score 0-100 + gating por régimen)  [FASE 3]
  ml/                features (vectorización + etiquetado), walkforward (split temporal
                     anclado + métricas), classifier (LR/RF/GB + calibración isotónica),
                     confidence (prob→ml_score), predictor (artefacto joblib),
                     model_manager (comparación + selección), synthetic (dataset offline)  [FASE 4]
  risk/              risk_engine (autoridad final sobre cada Signal), position_sizing
                     (qty = riesgo/distancia al stop), stop_loss (ATR/fijo con min-max),
                     take_profit (risk:reward), exposure (posiciones/total correlado),
                     portfolio (estado frozen de cuenta)  [FASE 5]
  execution/         base (ExchangeAdapter ABC), simulated (SimulatedExecutionAdapter:
                     fills LIMIT/STOP/TP por set_price con semántica Binance), order_manager
                     (idempotencia por client_order_id, retry con backoff, timeout→cancel,
                     cancel_and_replace), position_manager (ciclo de vida obligatorio con
                     SL+TP y abrir/cerrar), execution_router (risk→execution + build_portfolio),
                     reconciliation (Estado interno vs venue)  [FASE 6]
  trading/           paper_account (cash/PnL/fees + mark-to-market), orchestrator
                     (PaperTradingEngine es el ciclo completo señal→riesgo→ejecución con
                     guard por símbolo, pausa por errores Fatales, persistencia y summary),
                     reconciler (reconciliación periódica con mitigación: cierre de
                     desprotegidas, cancel de huérfanas, flatten de no-trackeadas)  [FASE 7]
  config/            settings (env externo), risk, symbols, strategies, execution, paper
  external_data/     NewsProvider plugin (null por defecto), NewsStore con TTL
  storage/           Repository (no-op) + SqliteRepository (audit trail idempotente:
                     positions/orders/fills/risk_decisions/reconciliations/events +
                     señales señal + heartbeats de observabilidad, con migraciones
                     aditivas)  [FASE 7 / 8.5]
  paper/             engine (PaperTradingEngine: source de snapshots → señal → orquestador,
                     con tope one-shot de trades y cierre ordenado)  [FASE 7]
  monitoring/        logger estructurado + redacción de secretos, metrics (p50/p95/p99),
                     alerts (TelegramNotifier fire-and-forget: cola acotada + worker,
                     sin bloquear el hot path), observability (envuelve el Repository:
                     alertas de transiciones SL/TP, halt de riesgo con cooldown, marks
                     para pnl_unrealized, heartbeat), dashboard_data (agregaciones puras
                     sobre SQLite para el dashboard)  [FASE 8.5]
  dashboard/         app.py — Streamlit independiente que lee SOLO la DB  [FASE 8.5]
  main.py            pipeline en tiempo real + modos one-shot / signal / paper / backtest
```

Pipeline por activo (Producer → Queue → Processor → State → Feature):

```
WebSocket(aggTrade+depth) → EventBus (buffers acotados) → SymbolProcessor
→ SymbolState (OrderBook L2 + TradeAggregator + CandleBuilder)
→ FeatureEngine (cada 1s) → FeatureSnapshot (features por categoría)
→ SignalEngine → Signal (score 0-100 + componentes) → log KV con provenance + métricas
```

Features clave por categoría (todas timestamped, sin look-ahead):

- **PRICE**: returns 1s/5s/15s/60s, rango, distancia a EMA9, precio vs VWAP, posición en banda de Bollinger.
- **VOLUME**: volumen, relative volume, z-score, aceleración, buy/sell, aggressive volume, buy ratio, divergencia.
- **VOLATILITY**: ATR(+%), realized vol, ancho de banda de Bollinger.
- **ORDER FLOW**: delta de flujo 5s/30s, imbalance de flujo, aggressive buy ratio, trades/s, tamaño medio.
- **ORDER BOOK**: OBI (+variación), microprice pressure, spread(+%), profundidad ±0.05/0.10/0.25%, ratio bid/ask.
- **MOMENTUM**: RSI, ROC 10/30/60.
- **TREND**: EMA9/21/50, stack + alineación, ADX, precio vs EMA50/VWAP.
- **SOCIAL**: mention z-score, conteo de noticias (placeholder real en fases posteriores).
- **NEWS**: sentimiento, peso de impacto, edad, TTL, relevancia.
- **MARKET REGIME**: régimen + flags (trending/range/breakout/extreme/high/low-vol/unknown).

El **Signal Engine** combina 8 dimensiones ponderadas (pesos en `config/strategies.py`,
configurables y **no declarados óptimos** — se validarán con backtest en FASE 8):
`trend, momentum, volume, order_book, volatility, price_structure, news, ml`
→ **Signal Score 0-100** → dirección LONG/SHORT/FLAT por umbrales configurables.
El régimen puede hacer *gating* (`allowed_regimes`). **La dimensión `ml` (FASE 4)**
no es un sobre-trade: cuando hay un `MLPredictor` conectado al `FeatureEngine`, la
dimensión refleja `P(up) vs P(down)` escalado a 0-100; sin predictor se mantiene
neutro (50) y el sistema funciona igual que en FASE 3.

El **Risk Engine (FASE 5)** es la autoridad final sobre el `Signal`:
`RiskEngine.assess(signal, portfolio_state)` evalúa, en orden y fail-fast,
kill switch / halted / safe mode, elegibilidad de estrategia, límite de pérdida
diaria, drawdown máximo, protección por pérdidas consecutivas, máximo de posiciones,
exposición total, exposición por grupo correlacionado (BTC/ETH/SOL...), **position
sizing** (`qty = riesgo/distance_to_stop`, con cap de apalancamiento y tick/lot size),
**stop-loss** (ATR-based con min/max) y **take-profit** (risk:reward ≥ ratio mínimo).
Devuelve un `RiskDecision`: APPROVED con `position_size/stop_loss_price/
take_profit_price/notional_value/risk_amount` + `risk_checks`, o REJECTED /
TRADING_HALTED / SAFE_MODE con `RejectReason` tipado. **Una señal elegible aún puede
ser rechazada por riesgo: el riesgo manda sobre estrategia y ML.** Es funcialmente
puro (sin red ni estado interno: `PortfolioState` es un dataclass frozen).

El **Execution Engine (FASE 6)** es la única capa que ejecuta (offline). La
jerarquía es innegociable: `Signal → RiskEngine.assess() → solo APPROVED ejecuta`;
REJECTED / TRADING_HALTED / SAFE_MODE jamás llegan al venue. `ExecutionRouter.route()`
probó el riesgo y, si APRUEBA, delega en el `PositionManager`, que impone la
secuencia irreversible `ENTRY_SUBMITTED → ENTRY_FILLED → PROTECTING → ACTIVE →
CLOSED/ABORTED` con la invariante dura: **una posición solo es ACTIVE con SL y TP
resting confirmados**; si la protección falla cierra por market reduce-only y si ese
cierre también falla levanta `PositionNotProtectedError` (Fatal) y marca halt.
`OrderManager` da idempotencia por `client_order_id` (replay de resultados, coalescing
de submits concurrentes), retries solo para `Recoverable`/`Transient` y timeout→cancel
best-effort. El `SimulatedExecutionAdapter` repite la semántica Binance Futures de
triggers (STOP_MARKET/TAKE_PROFIT_MARKET por lado, reduce_only clamp al net, fills con
slippage configurable y plans parciales). `build_portfolio_state()` reconstruye el
`PortfolioState` frozen que el RiskEngine consumirá en el bucle real, y
`ReconciliationEngine` compara libro interno vs venue
(MISSING_STOP/MISSING_TP/QTY_MISMATCH/ORPHAN_ORDER/UNTRACKED_POSITION).
**FASE 6 es 100% simulado: `build_execution_stack("live", ...)` lanza
`NotImplementedError`; ninguna orden real existe todavía.**

El **Paper Trading Engine (FASE 7)** une toda la cadena offline en un bucle
ejecutable `snapshot → Signal → RiskEngine → PositionManager → venue simulado`,
sin red y 100% determinista. `TradeOrchestrator.on_signal()` es el gate de nivel
aplicación (además del Risk Engine): bloquea el símbolo ya abierto
(`PAPER_MAX_OPEN_PER_SYMBOL`), rechaza señales FLAT y pausa el bot ante un error
Fatal de ejecución (`PositionNotProtectedError` / `ProtectionTimeoutError`).
`on_price()` empuja el tick al venue y hace mark-to-market con `PaperAccount`
(equity = cash + unrealized, comisiones taker por notional de entrada/salida).
`PeriodicReconciler` re-verifica libro interno vs venue en cadencia y **mitiga**
en vez de solo quejarse: posición sin SL/TP → cierre por market reduce-only
(`reconcile_missing_protection`), orden huérfana → cancel, exposición no
trackeada → flatten con una market reduce-only. Lo que sigue Fatals sin remediar
pausa el orquestador. Todo se persiste de forma idempotente en
`SqliteRepository` (positions/orders/fills/risk_decisions/reconciliations/
events) para auditar cualquier sesión, y `main.py --mode paper --trades N`
ejecuta una sesión acotada sobre el feed real sin dinero real.

## Cómo ejecutar

Requisitos: Python 3.11+ (probado con 3.14). Instalar dependencias:

```powershell
pip install -r requirements-dev.txt
```

1) **Verificación completa offline** (sin red):

```powershell
python -m pytest -q        # 392 tests
```

2) **Entrenamiento ML offline + end-to-end** (FASE 4 — datos sintéticos deterministas,
   sin red; compara LR/RF/GB con walk-forward, selecciona por Brier, evalúa en
   holdout temporal y verifica que SignalEngine consume la dimensión ml):

```powershell
python -m crypto_scalper.ml_train --seconds 180
```

3) **Smoke test end-to-end con datos reales** (imprime un FeatureSnapshot y sale):

```powershell
python -m crypto_scalper.main --mode one-shot
```

4) **Señal de ejemplo con desglose** (FASE 3 — snapshot + Signal Score + componentes):

```powershell
python -m crypto_scalper.main --mode signal --symbols BTCUSDT
```

5) **Pipeline en tiempo real** (FASE 2/3, no genera órdenes):

```powershell
python -m crypto_scalper.main --mode pipeline --symbols BTCUSDT,ETHUSDT,SOLUSDT
```

Sin `--symbols` construye el universo completo (~100 símbolos, el selector usa
el Tradability Score). En pipeline el Signal Engine evalúa cada snapshot y registra
la señal con provenance (features + sub-scores + motivo) en el log KV y métricas.
CTRL+C detiene de forma limpia.

5) **Sesión de paper trading acotada** (FASE 7 — sin dinero real; detiene tras `N`
    trades cerrados o con CTRL+C):

```powershell
python -m crypto_scalper.main --mode paper --symbols BTCUSDT --trades 5
```

El modo paper construye el pipeline real (WebSocket → Features → Signal), ejecuta
`on_price`/`on_signal` contra el `SimulatedExecutionAdapter`, reconcilia en
cadencia y deja el audit trail en el SQLite de `PAPER_DB_PATH` (`logs/paper.db`).

6) **Alertas Telegram + dashboard** (FASE 8.5 — en tu `.env` local, gitignored):

```powershell
# .env: token/chat de tu bot (BotFather) + MONITORING_TELEGRAM_ENABLED=true
python -m crypto_scalper.main --mode paper --symbols BTCUSDT
streamlit run crypto_scalper/dashboard/app.py   # lee logs/paper.db (DASHBOARD_DB_PATH)
```

El `TelegramNotifier` es fire-and-forget (nunca bloquea el ciclo de ejecución); si
`MONITORING_TELEGRAM_ENABLED=false` el bot funciona igual sin sendMessage. El
dashboard es un proceso separado que lee exclusivamente la SQLite (heartbeats con
equity/drawdown/exposición, posiciones abiertas, señales recientes y métricas).

7) **Entornos**: copia `.env.example` a `.env` (dev), o crea `.env.<APP_ENV>`.
   El modo LIVE bloquea el arranque si no existe `LIVE_TRADING_ENABLED=true`.

## Configuración aplicada por defecto

| Tema | Valor |
|---|---|
| Máximo riesgo por operación | 1% (cap duro; RiskConfig lo valida) |
| Apalancamiento | máx. 3x (cap duro; default 1x) |
| Riesgo total abierto | 3% del equity |
| Exposición por grupo correlacionado | 6% (grupo default: BTC/ETH/SOL) |
| Límite pérdida diaria | 3% (Risk Engine → TRADING_HALTED) |
| Max drawdown | 10% (Risk Engine → SAFE_MODE) |
| Protección pérdidas consecutivas | pausa tras 4-5, reduce riesgo tras 3 |
| Position sizing (FASE 5) | `qty = (equity × risk%) / stop_distance`, con cap de leverage + tick/lot |
| Stop loss (FASE 5) | ATR×1.5 (clamped a [0.05%, 5%]) o fijo configurable |
| Take profit (FASE 5) | risk:reward ≥ 1.5 (default 2.0) |
| Execution retries (FASE 6) | 2 (solo `Recoverable`/`Transient`, backoff 0.5s×2ⁿ cap 5s) |
| Timeouts ejecución (FASE 6) | submit 5 s · fill 10 s · protección SL+TP 10 s |
| Slippage simulado (FASE 6) | 0.05% del precio (configurable) |
| Equity paper (FASE 7) | 10 000 USDT (`PAPER_START_EQUITY`) |
| Comisión paper (FASE 7) | 0.04% taker sobre notional de entrada+salida (`PAPER_FEE_PCT`) |
| Cadencias paper (FASE 7) | reconciliación 60 s · resumen 30 s |
| Máx. posición abierta por símbolo (FASE 7) | 1 (`PAPER_MAX_OPEN_PER_SYMBOL`) |
| Audit trail (FASE 7) | SQLite en `logs/paper.db` (`PAPER_DB_PATH`) |
| Alertas Telegram (FASE 8.5) | `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` + `MONITORING_TELEGRAM_ENABLED` (false por defecto) |
| Cadencia heartbeat (FASE 8.5) | 5 s (`MONITORING_HEARTBEAT_INTERVAL_S`) · cola 256 · cooldown halt 300 s |
| Dashboard (FASE 8.5) | `streamlit run crypto_scalper/dashboard/app.py` · DB `DASHBOARD_DB_PATH`=`logs/paper.db` |
| Lote WS | 20 streams/conexión |
| Snapshot depth | 100 niveles/símbolo |
| Feature interval | 1 s |
| Pesos de señal (FASE 3) | trend 0.20 · momentum 0.20 · volume 0.15 · order_book 0.15 · volatility 0.10 · price_structure 0.10 · news 0.05 · ml 0.05 |
| Umbrales dirección | LONG ≥ 60 · SHORT ≤ 40 (configurables) |

Todo es configurable por variables de entorno; nada de riesgo/secrets está hardcodeado.

## Arquitectura y decisiones relevantes

- **Producer nunca se bloquea esperando análisis**: el WebSocket publica con
  `publish_nowait`; si una cola de consumidor se llena se marca resync del símbolo
  (nunca se pierde silenciosamente: log + métrica).
- **Order Book L2 sincronizado de verdad** (no reemplazo total por evento):
  snapshot REST + diff stream con validación `U / u / pu`, descarte de eventos
  antiguos, replay de eventos buffereados y resync automático ante gaps.
- **Signal Score no binario (FASE 3)**: 0-100 como suma ponderada de 8 dimensiones
  (cada una 0-100 con signo direccional); `Signal` lleva componentes + régimen +
  `eligible`/`reason` para trazabilidad total. Los pesos viven en `config/strategies.py`
  y **solo se validan en backtesting (FASE 8)**.
- **Gating por régimen**: una estrategia declara `allowed_regimes`; fuera de ellos el
  `Signal` se marca `regime_blocked` y no es elegible.
- **Risk Engine con autoridad final (FASE 5)**: un Signal elegible aún puede ser
  rechazado (`RISK_EXPOSURE_EXCEEDED`, `DAILY_LOSS_LIMIT`, `MAX_DRAWDOWN`,
  `CORRELATION_EXCEEDED`, etc.). Riesgo nunca es solo gating de estrategia: es la
  última puerta antes de ejecución. Determinista y offline (Estado frozen por
  `PortfolioState`, sin red ni estado interno).
- **Position sizing por riesgo, no porcentaje arbitrario** (FASE 5):
  `risk_amount = equity × risk_per_trade_pct` y `qty = risk_amount / stop_distance`
  (ajustada a leverage cap y tick/lot size). SL es ATR-based con min/max, TP por
  risk:reward con ratio mínimo desde config. Todo en `risk/`.
- **Ejecución offline e idempotente (FASE 6)**: la única capa que ejecuta es
  `execution/`. El `SimulatedExecutionAdapter` reproduce Binance Futures (triggers
  STOP_MARKET/TAKE_PROFIT_MARKET por lado, reduce_only clamp, parciales, evento por
  cambio). `PositionManager` impone la invariante SL+TP resting antes de ACTIVE y
  cierra por market ante pérdida de protección; `build_stack("paper", ...)` construye
  TODO el stack (adapter→om→pm→router) con 5 líneas. `build_stack("live", ...)`
  deliberadamente no existe (FASE 7).
- **Nunca sin SL+TP (FASE 6)**: una posición no puede pasar a ACTIVE si sus
  protecciones no están resting; si la protección se pierde en vivo, el manager
  cancela el resto y cierra por market reduce-only, y si el cierre falla marca
  `_trading_halted` y lanza `PositionNotProtectedError` (Fatal). Además, al cerrar
  por SL/TP el hermano que quedó resting se cancela (higiene del book) — FASE 7.
- **Orquestador de paper trading (FASE 7)**: `TradeOrchestrator` compone todo el
  ciclo y aplica sus propias políticas además del Risk Engine: nunca abre el mismo
  símbolo dos veces (`PAPER_MAX_OPEN_PER_SYMBOL`), una señal FLAT/neutra jamás toca
  el venue y un error Fatal de protección pausa el bot (nada de fire-and-forget).
  El equity que consume el riesgo es mark-to-market real (`PaperAccount`: cash +
  PnL + unrealized, comisiones por notional de entrada/salida).
- **Reconciliación que mitiga, no solo reporta (FASE 7)**: `PeriodicReconciler`
  cierra por market cualquier posición ACTIVE sin protección resting, cancela
  órdenes huérfanas y aplana exposure no trackeada (reduce-only); tras cada pase
  vuelve a chequear: lo que sigue siendo Fatal pausa al orquestador. Cada reporte
  se persiste para auditoría.
- **Audit trail idempotente (FASE 7)**: `SqliteRepository` guarda posiciones (upsert
  por `position_id`), órdenes (upsert por `client_order_id`), fills desduplicados,
  decisiones de riesgo y reconciliaciones en un solo archivo; `OrderManager.refresh()`
  sincroniza la caché con el venue antes de persistir para que el histórico sea fiel
  a lo que realmente pasó.
- **Tope one-shot de trades (FASE 7)**: `PaperTradingEngine.run(max_trades=N)`
  detiene la sesión al cerrar el trade N-ésimo (sin abrir uno extra) y persiste el
  estado final antes de cerrar el repo.
- **Observabilidad fuera del camino crítico (FASE 8.5)**: `TelegramNotifier` encola
  (`emit` con `put_nowait`) y un worker único hace el POST HTTP; cola llena → drop
  con métrica, jamás bloquea al `ExecutionRouter`/`TradeOrchestrator`. El
  `ObservabilityRepository` envuelve al Repository como observador puro: deriva
  TRADE_OPENED/CLOSED de las transiciones de estado que ya se persisten, RISK_HALT/
  KILL_SWITCH de las decisiones de riesgo (reason `DAILY_LOSS_LIMIT`/`MAX_DRAWDOWN`/
  `KILL_SWITCH`, verdict `SAFE_MODE`) con cooldown anti-spam, calcula
  `pnl_unrealized` con el mark del adapter/venue (duck-typed `attach_mark`) y
  persiste heartbeats (equity = start + realized + unrealized, drawdown, exposición,
  trades del día). Los motores de Riesgo/Ejecución no se tocan: el observador entra
  por la capa de persistencia que el orquestador ya usa. `--mode backtest` escribe
  heartbeats pero tiene alertas desactivadas.
- **Audit de señales (FASE 8.5)**: el orquestador persiste cada señal con su motivo
  de rechazo (`signal_score` + `risk_rejection_reason`); las señales bloqueadas por
  pause/símbolo-abierto/FLAT también quedan registradas para el dashboard.
- **Features por categoría sin look-ahead**: el FeatureEngine emite 10 categorías
  (`features/categories.py`), todas con el mismo `timestamp_ms` de decisión; las
  ventanas de trades/nivel usan `now_ms`, y los indicadores solo leen ventanas
  pasadas. Sin predictor conectado la dimensión `ml` es neutra (50) y nada cambia;
  con `MLPredictor` conectado, la categoría `ml` se inyecta en el snapshot y el
  scorer la utiliza (FASE 4).
- **Cambio de proveedor de noticias sin tocar estrategia**: protocolo `NewsProvider`.
  En FASE 2 solo existe `NullProvider`; el motor de features ya está preparado
  para fusionar `NewsEvent` con TTL.
- **Sin LLM en el camino crítico**: el pipeline de features es NumPy/asyncio puro.
- **persistencia**: la capa `Repository` existe pero es no-op (decisión explícita:
  no volcar ticks crudos a una BD en esta fase).

## Riesgos y limitaciones conocidas (FASE 2/3/4/5)

1. Los `aggTrades` REST (modo one-shot/signal) **no traen lado agresor** → buy/sell=0
   y ORDER FLOW=0 en esos modos; el canal WebSocket `@aggTrade` sí lo trae (m),
   por lo que en el pipeline real sí se calculan.
2. Indicadores sobre velas de 1s son ruidosos por diseño; los pesos del Signal Score
   **no son óptimos** y el régimen se calibrará con backtest (FASE 8). **Nada aquí
   afirma rentabilidad.**
3. El universo se re-selecciona solo al arranque (refresh periódico programado
   para fases posteriores).
4. **Gating por régimen**: el Signal Engine marca no-eligible, y el Risk Engine
   (FASE 5) puede rechazar aunque sea elegible. Aún no hay ejecución real (FASE 7):
   la FASE 6 solo ejecuta contra el simulador (sin red, sin órdenes reales).
5. El flujo de datos asume un feed monótono: las features se evalúan en el instante
   de decisión sobre los eventos ya recibidos (no hay replay histórico de decisión).
6. Microestructura avanzada (imbalance diffs, detección de anomalías, latencia
   end-to-end completa) y ejecución son fases posteriores no implementadas.
7. El streaming usa `@depth` (diff) a ~100 updates/seg por símbolo; para 100
   símbolos reales habrá que medir la presión sobre `DELTA depth` y ajustar lote.
8. **ML (FASE 4) aún no es backtesting**: el clasificador se entrena/valida offline
   con datos sintéticos deterministas (sin red). Las métricas OOS son plausibles,
   pero la utilidad real solo se mide en backtest (FASE 8) y paper trading (FASE 7).
   La calibración isotónica en folds pequeños puede degradar la probabilidad de
   `logistic`; la selección por Brier lo refleja honestamente y prefiere RF/GB.
9. **Risk Engine (FASE 5) es offline por diseño**: `assess()` recibe un
   `PortfolioState` frozen como snapshot; la actualización del estado real
   (posiciones abiertas, PnL daily, equity tras fills) la hará el Execution/Position
   manager en FASE 6-7. Los valores de SL/TP/sizing son la entrada recomendada a la
   ejecución, todavía sin costes/slippage (FASE 8).
10. El modelo de costes (`Expected Edge > Minimum Required Edge`) queda marcado como
    gate habilitado; su cómputo completo (fees+spread+slippage+funding) es FASE 8.
11. **Execution (FASE 6) es offline por diseño**: `build_execution_stack("live", ...)`
    lanza `NotImplementedError`. El `SimulatedExecutionAdapter` es determinista
    (fills al precio de trigger con slippage configurable), sin latencia, comisiones,
    funding, profundidad real ni riesgo de contra-partida.
12. La reconciliación (FASE 6-7) compara libro interno vs venue del simulador y ya
    mitiga la deriva (cierre de desprotegidas, cancel de huérfanas, flatten de
    no-trackeadas); un adapter real tendrá reconciliación de IDs como tarea inicial
    de la fase LIVE.
13. **Paper trading (FASE 7) es simulado por diseño**: equity, comisiones y fills
    vienen del `PaperAccount` + `SimulatedExecutionAdapter` (deterministas, sin
    latencia ni profundidad real); los pesos de señal siguen sin validar en backtest.
    La sesión `--mode paper` consume el feed real de WebSocket: si el feed se corta,
    la sesión se detiene limpiamente y el audit trail de `logs/paper.db` queda intacto.
14. **Observabilidad (FASE 8.5) es solo lectura/notificación**: las alertas Telegram
    dependen de conectividad con `api.telegram.org` (se descartan por cola llena o
    timeout sin afectar al bot); el dashboard lee la SQLite en modo readonly y si la
    DB no existe o aún no hay heartbeats muestra un aviso. Los PnL y marks son
    aproximaciones del simulador (sin funding/latencia real) — misma limitación que
    FASE 8.

## Verificación manual

- `pytest` en verde (392 tests).
- `python -m crypto_scalper.ml_train --seconds 180` produce un reporte de
  comparación walk-forward (LR vs RF vs GB, selección por Brier, holdout temporal
  OOS) + verificación end-to-end de que SignalEngine usa la dimensión `ml`
  (score ≠ 50 cuando el modelo predice con direccionalidad).
- Los tests `tests/test_risk_*.py` (69 tests) verifican el Risk Engine offline:
  sizing por riesgo con cap de leverage/tick/lot, SL ATR con min/max, TP por
  risk:reward, exposición (posiciones/total/correlación), PortfolioState y la
  jerarquía riesgo > estrategia (un Signal elegible puede ser rechazado).
- Los tests `tests/test_execution_*.py` (65 tests) verifican el Execution Engine
  offline: fills del simulador (límites/stops/TP/parciales/reduce_only), semántica
  Binance de triggers, idempotencia/retry/timeout del OrderManager, ciclo de vida
  obligatorio del PositionManager con la invariante SL+TP resting, PnL exacto por
  SL/TP, entrada parcial, protección perdida→cierre, rebuild de PortfolioState, y
  que el router jamás ejecuta REJECTED/HALTED/SAFE_MODE y que la reconciliación
  detecta stops/TP faltantes, mismatches y órdenes huérfanas.
- Los tests `tests/test_paper_*.py`, `test_trading_*.py`, `test_storage_sqlite.py`
  (44 tests) verifican el Paper Trading Engine (FASE 7): equity/PNL/fees/mark-to-market
  del `PaperAccount`, el ciclo completo señal→riesgo→ejecución del `TradeOrchestrator`
  (aprobado abre posición protegida, rechazado no toca el venue, guard por símbolo,
  TP→ganancia, SL→pérdida, errores Fatales→pausa), la mitigación del
  `PeriodicReconciler` (stop perdido→cierre, huérfana→cancel, no-trackeada→flatten,
  QTY_MISMATCH irresoluble→pausa), el roundtrip/idempotencia/reopen del
  `SqliteRepository` y sesiones deterministas del engine con tope de trades.
- `--mode one-shot` produce JSON con `symbol/price/vwap/rsi/atr/volume_zscore/
  order_book_imbalance/spread_pct/microprice/regime/...`.
- `--mode signal` añade `signal.score` (0-100), `signal_type`, `eligible` y el
  desglose por dimensión con sus weights y features.
- `--mode pipeline` / `--mode paper`: el log muestra `signal generated symbol=...
  score=... side=...` con provenance; en paper se añaden las decisiones del Risk
  Engine, posiciones SL/TP, sumarios de equity y reconciliaciones, y al terminar
  `logs/paper.db` permite auditar positions/orders/fills/risk_decisions.
- `streamlit run crypto_scalper/dashboard/app.py` muestra el estado del bot sin tocar
  los procesos en marcha (solo lectura de la DB): equity/drawdown, posiciones
  abiertas, últimas señales y métricas de riesgo.

## Siguientes fases

La **FASE 8 (backtesting)** reproduce el histórico por el stack real
(Features → Señal → Risk → Ejecución con costes por barra y `BarVenue`
determinista) y la **FASE 8.5 (observabilidad)** añade las alertas Telegram
fire-and-forget, el dashboard Streamlit sobre SQLite y el audit de señales con
motivo de rechazo. Queda como pendiente principal la **FASE 9: adapter LIVE** de
Binance Futures real (marcado pero nunca activo sin `LIVE_TRADING_ENABLED=true`),
más calibración de pesos del Signal Score con el backtest y el estado de cuenta
enriquecido del dashboard.