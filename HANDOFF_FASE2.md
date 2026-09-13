# HANDOFF — cierre de sesión (FASE 1 + FASE 2)

Fecha: 2026-09-13
Proyecto: crypto_scalper (scalping cuantitativo asíncrono sobre Binance USDⓈ-M Futures)
Estado: FASE 1 (arquitectura/riesgo/diseño) y FASE 2 (ingesta y procesamiento en tiempo real) COMPLETAS y verificadas.

---

## 1. Resumen de lo completado

### FASE 1 — Arquitectura, métricas, riesgo y diseño
- Configuración 100% externa por entorno (`dev` / `paper` / `live`), validada y tipada (`Settings.load()`).
- Barrera explícita `LIVE_TRADING_ENABLED=false` por defecto; el modo LIVE se niega a arrancar sin el flag.
- Dominio tipado: enums, modelos timestamped, jerarquía de excepciones (recoverable / transient / fatal).
- `EventBus` pub/sub con colas asíncronas acotadas (backpressure real, overflow explícito).
- Riesgo definido y validado (`RiskConfig`): riesgo/trade 1% cap duro, leverage ≤3x, daily loss 3%, drawdown 10%. Consumido por FASE 5.
- Observabilidad: logging estructurado KV con redacción de secretos y metrics en proceso (counters/gauges/hist p50/p95/p99).

### FASE 2 — Ingesta de datos y procesamiento en tiempo real
- Pipeline Producer → Queue → Processor → State → Features por símbolo (aprox. 100 símbolos).
- REST weight-aware (token bucket 2400/min) para snapshots de order book, exchangeInfo, ticker 24h y aggTrades.
- WebSocket combined stream (`@aggTrade` + `@depth`), agrupado por lotes configurables, reconnect con backoff exponencial + jitter, ping/pong, detección de feed stall por timeout de recepción.
- Order Book L2 **realmente sincronizado**: snapshot REST + diffs con validación `U/u/pu`, descarte de eventos antiguos, buffer+replay, detección de gaps y resync automático.
- Agregación de trades en ventanas (1s, 5s, 15s, 30s, 1m, 5m) con buy/sell por lado agresor, z-scores y RVOL; bucles eficientes (sin pandas).
- Candles de 1s construidas desde el stream aggTrade.
- Indicadores en NumPy puro: EMA 9/21/50, RSI (Wilder), ATR (Wilder), ADX, Bollinger, ROC, VWAP (sesión UTC, reset explícito).
- Universe Selector con Tradability Score (quote volume, spread, depth, actividad) y filtros duros; produce el universo ~100 activos.
- FeatureEngine (FusionEngine) que combina mercado + noticias con TTL y produce `FeatureSnapshot` estructurado.
- MarketRegime: TRENDING_UP/DOWN, RANGE, HIGH/LOW_VOLATILITY, BREAKOUT, EXTREME, UNKNOWN.
- Noticias vía plugin `NewsProvider` (por defecto `NullProvider`); almacenamiento tras `Repository` no-op (decidido: no volcar ticks crudos a BD en esta fase).

### Verificación hecha
- 68/68 tests pytest en verde.
- `--mode one-shot`: conectó con Binance en vivo y produjo el FeatureSnapshot real de 1000PEPEUSDT.
- `--mode pipeline --symbols BTCUSDT,ETHUSDT,SOLUSDT`: 3 orderbooks sincronizados, WS conectado, 0 errores en 25 s.

---

## 2. Archivos creados y responsabilidad principal

### Configuración
| Archivo | Responsabilidad |
|---|---|
| `config/settings.py` | `Settings` inmutable desde env (dev/paper/live); `WebSocketConfig`, `UniverseConfig`, `FeatureConfig`. |
| `config/risk.py` | `RiskConfig` + caps duros (1% riesgo/trade, 3x leverage) + `EnvironmentLimits`. |
| `config/symbols.py` | `SymbolRules`: quotes permitidos, bloqueos, estados no tradables. |
| `config/strategies.py` | `StrategyConfig` con pesos de señal por defecto (FASE 3 los usará). |

### Core
| Archivo | Responsabilidad |
|---|---|
| `core/models.py` | `AggTrade`, `DiffDepthEvent`, `Candle`, `OrderBookMetrics`, `NewsEvent`, `FeatureSnapshot`, `OrderRequest`, `RiskDecision`. |
| `core/events.py` | `EventBus` pub/sub acotado + subtipos `TradeEvent`/`DepthEvent`/`FeatureEvent`/`LifecycleEvent`. |
| `core/enums.py` | Regime, Side, AggressorSide, Impact, RiskVerdict, etc. |
| `core/exceptions.py` | Jerarquía tipada recoverable/transient/fatal. |

### Market data
| Archivo | Responsabilidad |
|---|---|
| `market_data/rest.py` | Cliente REST públic o (interfaz que vive en FASE 6): `depth`, `agg_trades`, `ticker_24h`, `exchange_info`, `klines`; `RateLimiter`. |
| `market_data/websocket.py` | Producer WS (`WebSocketManager`): combined streams, parseo a eventos tipados, reconnect/backoff, ping/pong, `publish_nowait`. |
| `market_data/orderbook.py` | `OrderBook` L2 sincronizado (`apply_snapshot`/`apply_diff`) + `metrics()` → `OrderBookMetrics`. |
| `market_data/trades.py` | `TradeAggregator` ventanas 1s–5m, z-score, RVOL. |
| `market_data/candles.py` | `CandleBuilder` 1s desde trades + `CandleSeries` numpy. |
| `market_data/universe.py` | `UniverseSelector` + `rank()` puro (Tradability Score) + `_book_metrics`. |
| `market_data/state.py` | `SymbolState` (book+trades+candles+latest price). |
| `market_data/processor.py` | `SymbolProcessor`: drena colas por símbolo, mantiene state, resync de book. |

### Features
| Archivo | Responsabilidad |
|---|---|
| `features/technicals.py` | EMA, RSI, ATR, ADX, SMA, Bollinger, ROC, session/rolling VWAP, realized vol. |
| `features/price_features.py` | returns, rango, distancia a EMA. |
| `features/volume_features.py` | buy/sell volume, z-score, RVOL, acceleration, divergence. |
| `features/volatility.py` | ATR + ATR%, realized vol. |
| `features/regime.py` | `MarketRegime` + `RegimeThresholds`. |
| `features/feature_engine.py` | `FeatureEngine.compute(state) -> FeatureSnapshot` (fusión mercado+noticias con TTL). |

### Otros
| Archivo | Responsabilidad |
|---|---|
| `external_data/base.py` | Protocolo `NewsProvider`, `NullProvider`, `classify_impact`, `symbol_relevance`. |
| `external_data/store.py` | `NewsStore` con TTL para fusión. |
| `storage/base.py` | `Repository` ABC + `NoopRepository`. |
| `monitoring/logger.py` | Logging KV con redacción de secretos. |
| `monitoring/metrics.py` | Registry en proceso (p50/p95/p99). |
| `main.py` | CLI: `--mode pipeline|one-shot`, `--symbols`, `--env`. |

### Tests (68) — `tests/`
`test_technicals.py`, `test_orderbook.py`, `test_trades.py`, `test_events.py`, `test_settings.py`, `test_regime.py`, `test_universe.py`, `test_feature_engine.py`.

---

## 3. Decisiones técnicas y limitaciones conocidas

### Decisiones clave
- **aggTrades vía WebSocket (`@aggTrade`)**: la única fuente del lado agresor (`m`) en tiempo real. Los `aggTrades` REST (modo one-shot) NO traen `m` → buy/sell = 0 solo en one-shot.
- **Combined stream por lotes**: `WS_BATCH_SIZE=20` streams/conexión por defecto (aggTrade+depth → 10 conexiones para 100 símbolos). URL: `?streams=sym@aggTrade/sym@depth/...`.
- **Producer no bloqueante**: WS publica con `publish_nowait`; overflow de cola acotada → métrica + señal de resync, nunca pérdida silenciosa.
- **Order book**: claves de precio canónicas `_price_key` (`f"{p:.10g}"`) para que snapshot y diffs coincidan; cantidades coerced a float.
- **VWAP**: sesión UTC (reset a mediodía 00:00 UTC), típico price. `rolling_vwap` para ventanas.
- **RSI/ATR/ADX**: smoothing Wilder sobre velas de 1s; ATR/ADX requieren ≥ (periodo+1) velas.
- **Universe**: el endpoint `/fapi/v1/ticker/24hr` NO trae `baseAsset`/`quoteAsset` → se hace join con `/fapi/v1/exchangeInfo` (fix aplicado).
- **Microprice** definido sobre las cantidades del mejor nivel (bid·askQty + ask·bidQty)/topDenom.
- **Sin pandas en el hot path**; indicadores a ~1 Hz/símbolo sobre arrays.

### Limitaciones conocidas (punto 9 original)
1. buy/sell=0 en modo one-shot (REST aggTrades sin agresor); en pipeline real el WS sí lo calcula.
2. Indicadores sobre velas de 1s son ruidosos; calibración régimen/estrategia es FASE 3+8. **No hay afirmación de rentabilidad.**
3. El universo solo se (re)selecciona al arranque; refresh periódico pendiente.
4. Para ~100 símbolos conviene medir presión del stream `@depth` y ajustar `WS_BATCH_SIZE`.
5. ML, Risk Engine, ejecución, persistencia real y latencia end-to-end son fases posteriores.

### Errores corregidos en la sesión (no reintroducir)
- `FeatureConfig` ahora tiene defaults (`interval_s=1.0`, `rolling_history_size=120`).
- `CandleBuilder` usa `_MutableCandle` interno (no mutar dataclass frozen `Candle`).
- `_apply_levels` coerces a `float`; keys canonicalizadas.
- `NewsEvent.age_ms` es método (no era property).
- Formatter de logs renderiza cualquier atributo `extra=` (antes solo `extra_fields`).
- `ticker_24h` necesita join con exchangeInfo para base/quote.

---

## 4. Prompt exacto para arrancar FASE 3 en la nueva sesión

> Sigue el protocolo del documento de FASE 1/2 (los 10 puntos: qué construimos, archivos afectados, decisiones, implementación, instrucciones de ejecución, tests, verificación, riesgos, y detente).
>
> Estamos en el proyecto `C:\MCP\binancebot` (estado: FASE 1 y FASE 2 completas y testadas; 68 tests en verde; pipeline Producer→Queue→Processor→State→Feature hasta `FeatureSnapshot`).
>
> Implementa únicamente FASE 3 — "Motor de features y signal scores" — sin romper lo ya hecho:
>
> 1. Expande `FeatureEngine` para emitir todas las features por categoría (PRICE, VOLUME, VOLATILITY, ORDER FLOW, ORDER BOOK, MOMENTUM, TREND, SOCIAL, NEWS, MARKET REGIME), todas timestamped y sin look-ahead bias. Reutiliza `features/technicals.py`, `features/volume_features.py`, `features/price_features.py`, `features/volatility.py`, `features/regime.py`, `market_data/trades.py` y `market_data/orderbook.py`; NO reintroduzcas los bugs corregidos (ver HANDOFF_FASE2.md §3).
> 2. Implementa el Signal Engine con `Signal Score` 0–100 como puntuación ponderada configurable (weigths viven en `config/strategies.py`; no afirmes que son óptimos, requerirán backtesting en FASE 8). El score no es binario: emite señal + componentes por dimensión (trend/momentum/volume/order_book/volatility/price_structure/news/ml).
> 3. El régimen de mercado ya clasificado (`features/regime.py`) debe poder hacer *gating*: una estrategia puede no operar en un régimen.
> 4. Esta fase NO genera órdenes, NO ejecuta trades y NO consume ML (eso es FASE 4). Mantén `LIVE_TRADING_ENABLED=false` y no toques `RiskConfig`.
> 5. Entrega tests nuevos (pytest) para: scores por categoría, gating por régimen, ponderación configurable y ausencia de look-ahead. Corre `python -m pytest -q` completo (debe seguir en verde 68+ nuevos) y añade un modo de verificación (puedes extender `--mode one-shot` o añadir uno) que muestre una señal de ejemplo con su desglose.
> 6. Todo el logging sigue el formato KV con redacción de secretos; registra cada señal con su provenance (features → score → riesgo si aplica).
> 7. Al terminar, actualiza `README.md` y `HANDOFF_FASE2.md`/crea `HANDOFF_FASE3.md`, y detente esperando autorización para FASE 4.

---

## Cómo cerrar la sesión actual (hecho)
- Rama `main`, remoto `https://github.com/arturoylarreta-collab/binancebot.git`.
- Código subido a GitHub (ver estado del repo tras push).
- Este documento versta guardado como `HANDOFF_FASE2.md` en la raíz del proyecto.