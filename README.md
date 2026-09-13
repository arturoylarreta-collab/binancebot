# crypto_scalper — quantitative, async, modular crypto scalping system

Status: **FASE 1 (arquitectura/riesgo) + FASE 2 (ingesta de datos) + FASE 3 (motor de features y señales) + FASE 4 (filtro ML) implementadas.**

FASE 2-4 producen un `FeatureSnapshot` estructurado por activo con features por
categoría, un `Signal` Score 0-100 con desglose por dimensión y una dimensión
`ml` alimentada por un clasificador probabilístico ensayado offline con walk-forward
validation. Todo se entrena/evalúa con datos sintéticos deterministas (sin red).
**No genera órdenes ni ejecuta trades.**

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
  external_data/     NewsProvider plugin (null por defecto), NewsStore con TTL
  storage/           Repository (no-op en FASE 2)
  monitoring/        logger estructurado + redacción de secretos, metrics (p50/p95/p99)
  main.py            pipeline en tiempo real + modos one-shot / signal
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

## Cómo ejecutar

Requisitos: Python 3.11+ (probado con 3.14). Instalar dependencias:

```powershell
pip install -r requirements-dev.txt
```

1) **Verificación completa offline** (sin red):

```powershell
python -m pytest -q        # 160 tests
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

5) **Entornos**: copia `.env.example` a `.env` (dev), o crea `.env.<APP_ENV>`.
   El modo LIVE bloquea el arranque si no existe `LIVE_TRADING_ENABLED=true`.

## Configuración aplicada por defecto

| Tema | Valor |
|---|---|
| Máximo riesgo por operación | 1% (cap duro; RiskConfig lo valida) |
| Apalancamiento | máx. 3x (cap duro; default 1x) |
| Límite pérdida diaria | 3% (fase 5 consumirá esto) |
| Max drawdown | 10% |
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

## Riesgos y limitaciones conocidas (FASE 2/3/4)

1. Los `aggTrades` REST (modo one-shot/signal) **no traen lado agresor** → buy/sell=0
   y ORDER FLOW=0 en esos modos; el canal WebSocket `@aggTrade` sí lo trae (m),
   por lo que en el pipeline real sí se calculan.
2. Indicadores sobre velas de 1s son ruidosos por diseño; los pesos del Signal Score
   **no son óptimos** y el régimen se calibrará con backtest (FASE 8). **Nada aquí
   afirma rentabilidad.**
3. El universo se re-selecciona solo al arranque (refresh periódico programado
   para fases posteriores).
4. **Gating por régimen**: el Signal Engine marca no-eligible, pero aún no hay Risk
   Engine (FASE 5) ni ejecución (FASE 6): nada puede operar en FASE 3.
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

## Verificación manual

- `pytest` en verde (160 tests).
- `python -m crypto_scalper.ml_train --seconds 180` produce un reporte de
  comparación walk-forward (LR vs RF vs GB, selección por Brier, holdout temporal
  OOS) + verificación end-to-end de que SignalEngine usa la dimensión `ml`
  (score ≠ 50 cuando el modelo predice con direccionalidad).
- `--mode one-shot` produce JSON con `symbol/price/vwap/rsi/atr/volume_zscore/
  order_book_imbalance/spread_pct/microprice/regime/...`.
- `--mode signal` añade `signal.score` (0-100), `signal_type`, `eligible` y el
  desglose por dimensión con sus weights y features.
- `--mode pipeline`: el log muestra `signal generated symbol=... score=... side=...`
  con provenance, además de `orderbook synced` / `ws connected` sin overflow.

## Siguientes fases

FASE 4 (ML) quedó como filtro probabilístico: consumirá las 10 categorías de
`FeatureSnapshot`, nunca controla ejecución, y su artefacto (joblib) puede
reciclarse en producción. El Risk Engine (FASE 5) consumirá `RiskConfig`,
incluido el límite de pérdida diaria. Execution (FASE 6) añadirá credenciales
separadas por entorno, sin mezclarlas con este código.