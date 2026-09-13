# crypto_scalper — quantitative, async, modular crypto scalping system

Status: **FASE 1 (arquitectura/riesgo) + FASE 2 (ingesta de datos en tiempo real) implementadas.**

FASE 2 produce un `FeatureSnapshot` estructurado por activo a partir de datos reales
de Binance USDⓈ-M Futures. **No genera órdenes ni ejecuta trades.**

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
                     volatility, feature_engine (FusionEngine), regime
  external_data/     NewsProvider plugin (null por defecto), NewsStore con TTL
  storage/           Repository (no-op en FASE 2)
  monitoring/        logger estructurado + redacción de secretos, metrics (p50/p95/p99)
  main.py            pipeline en tiempo real + modo one-shot
```

Pipeline por activo (Producer → Queue → Processor → State → Feature):

```
WebSocket(aggTrade+depth) → EventBus (buffers acotados) → SymbolProcessor
→ SymbolState (OrderBook L2 + TradeAggregator + CandleBuilder)
→ FeatureEngine (cada 1s) → FeatureSnapshot → sink (log/metrics/repositorio)
```

Featros clave que ya producen los snapshots: precio, VWAP, RSI, ATR(+%), EMA9/21/50,
ADX, Bollinger, ROC, z-score de volumen, volumen relativo, buy/sell volume,
aggressive volume, order book imbalance, microprice, spread(+%), profundidad ±0.05/0.10/0.25%,
sentimiento/impacto/edad de noticias (TTL), mention z-score y régimen de mercado
(TRENDING_UP/DOWN, RANGE, HIGH/LOW_VOLATILITY, BREAKOUT, EXTREME, UNKNOWN).

## Cómo ejecutar

Requisitos: Python 3.11+ (probado con 3.14). Instalar dependencias:

```powershell
pip install -r requirements-dev.txt
```

1) **Verificación completa offline** (sin red):

```powershell
python -m pytest -q        # 68 tests
```

2) **Smoke test end-to-end con datos reales** (imprime un FeatureSnapshot y sale):

```powershell
python -m crypto_scalper.main --mode one-shot
```

3) **Pipeline en tiempo real** (FASE 2, no genera órdenes):

```powershell
python -m crypto_scalper.main --mode pipeline --symbols BTCUSDT,ETHUSDT,SOLUSDT
```

Sin `--symbols` construye el universo completo (~100 símbolos, el selector usa
el Tradability Score). CTRL+C detiene de forma limpia.

4) **Entornos**: copia `.env.example` a `.env` (dev), o crea `.env.<APP_ENV>`.
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

Todo es configurable por variables de entorno; nada de riesgo/secrets está hardcodeado.

## Arquitectura y decisiones relevantes

- **Producer nunca se bloquea esperando análisis**: el WebSocket publica con
  `publish_nowait`; si una cola de consumidor se llena se marca resync del símbolo
  (nunca se pierde silenciosamente: log + métrica).
- **Order Book L2 sincronizado de verdad** (no reemplazo total por evento):
  snapshot REST + diff stream con validación `U / u / pu`, descarte de eventos
  antiguos, replay de eventos buffereados y resync automático ante gaps.
- **Cambio de proveedor de noticias sin tocar estrategia**: protocolo `NewsProvider`.
  En FASE 2 solo existe `NullProvider`; el motor de features ya está preparado
  para fusionar `NewsEvent` con TTL.
- **Sin LLM en el camino crítico**: el pipeline de features es NumPy/asyncio puro.
- **persistencia**: la capa `Repository` existe pero es no-op (decisión explícita:
  no volcar ticks crudos a una BD en esta fase).

## Riesgos y limitaciones conocidas (FASE 2)

1. Los `aggTrades` REST (modo one-shot) **no traen lado agresor** → buy/sell=0 en
   ese modo; el canal WebSocket `@aggTrade` sí lo trae (m), por lo que en el
   pipeline real buy/sell sí se calculan.
2. Indicadores sobre velas de 1s son ruidosos por diseño; el régimen/estrategia se
   calibrarán y validarán en FASE 3 con backtest (FASE 8). **Nada aquí afirma rentabilidad.**
3. El universo se re-selecciona solo al arranque (refresh periódico programado
   para fases posteriores).
4. Microestructura avanzada (order flow, imbalance diffs, detección de anomalías,
   latencia end-to-end completa) y ML/ejecución son fases posteriores no implementadas.
5. El streaming usa `@depth` (diff) a ~100 updates/seg por símbolo; para 100
   símbolos reales habrá que medir la presión sobre `DELTA depth` y ajustar lote.

## Verificación manual

- `pytest` en verde (68 tests).
- `--mode one-shot` produce JSON con `symbol/price/vwap/rsi/atr/volume_zscore/
  order_book_imbalance/spread_pct/microprice/regime/...`.
- `--mode pipeline`: el log muestra `orderbook synced symbol=...`, `ws connected`
  y ningún `ws queue overflow` / `orderbook resync failed`.

## Siguientes fases

FASE 3 (motor de features y señales) y FASE 4 (ML) usarán los `FeatureSnapshot`
ya producidos. El Risk Engine (FASE 5) consumirá `RiskConfig`. Execution (FASE 6)
añadirá credenciales separadas por entorno, sin mezclarlas con este código.