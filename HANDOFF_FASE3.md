# HANDOFF — cierre de sesión (FASE 3)

Fecha: 2026-09-13
Proyecto: crypto_scalper
Estado: **FASE 1 + FASE 2 + FASE 3 completas y testadas (103 tests en verde).**

---

## 1. Resumen de lo completado en FASE 3

### Feature Engine — features por categoría
- `FeatureEngine` ahora emite `features_by_category` con 10 categorías:
  `PRICE`, `VOLUME`, `VOLATILITY`, `ORDER FLOW`, `ORDER BOOK`,
  `MOMENTUM`, `TREND`, `SOCIAL`, `NEWS`, `MARKET REGIME`.
- Cada categoría es un `Dict[str, float]` (más `regime_name` string), emitida por
  funciones puras en `features/categories.py` reutilizando los módulos existentes
  y un nuevo `features/orderflow.py`.
- Variación de OBI (`obi_delta`) se mantiene en memoria por símbolo en FeatureEngine.
- `NewsStore` expone `ttl_ms` property y `recent_count(symbol, now_ms)`.
- `FeatureSnapshot` ahora incluye `features_by_category: Dict[str, Dict[str, float]]`.
- **No look-ahead**: indicadores y ventanas usan exclusivamente datos ≤ `now_ms`.

### Signal Engine (strategies/)
- `strategies/signal_engine.py`: `SignalEngine.evaluate(FeatureSnapshot) -> Signal`.
- Cada dimensión (trend, momentum, volume, order_book, volatility, price_structure,
  news, ml) tiene un scorer puro que mapea `Dict → float [0, 100]`.
- El score final = suma ponderada de 8 sub-scores usando pesos de `StrategyConfig`.
- `Signal` lleva: `symbol`, `timestamp_ms`, `signal_type` (LONG/SHORT/FLAT),
  `score`, `regime`, `eligible`, `reason`, `components` (desglose 8 dimensiones).
- Regime gating: `allowed_regimes` tuple en StrategyConfig; fuera → `eligible=False`,
  `reason="regime_blocked:<regime>"`, `signal_type=FLAT`.
- ML neutro (50.0) hasta FASE 4, registrado explícitamente en scorer.

### StrategyConfig (config/strategies.py)
- Nuevos campos: `long_threshold` (60), `short_threshold` (40), `allowed_regimes`.
- Se valida **siempre** (no solo cuando `enabled=True`): suma de pesos = 1.0,
  las 8 dimensiones presentes, umbrales ordenados, regímenes conocidos.

### Settings (config/settings.py)
- Campo `strategies: StrategyConfig` con defaults seguros.
- Env vars: `STRATEGY_ENABLED`, `SIGNAL_WEIGHTS_JSON` (JSON de 8 weights),
  `STRATEGY_ALLOWED_REGIMES` (comma list), `STRATEGY_LONG_THRESHOLD`,
  `STRATEGY_SHORT_THRESHOLD`.

### main.py
- Nuevo modo `--mode signal`: descarga snapshot real + evalúa y muestra
  el Signal con desglose completo.
- Pipeline en tiempo real: después de cada `FeatureSnapshot`, el `SignalEngine`
  evalúa y registra `signal generated` con provenance en log KV + métricas
  (`signals.evaluated`, `signals.eligible`, `signals.gated`).

### Tests nuevos (35)
`tests/test_feature_categories.py` (13 tests):
- Todas las 10 categorías presentes, valores escalares,
  keys específicas por categoría, order flow buy bias, momentum uptrend,
  trend alignment, order book keys, news fusion, regime flags,
  **causal reproduction** (truncación de series a instant de decisión),
  features-bound-to-timestamp.

`tests/test_signal_engine.py` (15 tests):
- Score bounds, weighted sum, componentes default, ML neutral,
  LONG direction, SHORT direction, momentum-only weight, changing weights,
  regime gating blocked/allowed, disabled strategy, fresh/expired news,
  weights validation (sum/dims/thresholds/regime).

`tests/test_settings.py` (5 tests nuevos):
- Strategy defaults, full JSON weight override, partial override rejected,
  allowed regimes parse, unknown regime rejected.

### Actualizaciones
- `.env.example`: sección FASE 3 (strategy env vars documentadas).
- `README.md`: actualizado con FASE 3, modos, tablas, arquitectura, limitaciones.

---

## 2. Archivos creados (FASE 3)

| Archivo | Responsabilidad |
|---|---|
| `features/orderflow.py` | ORDER FLOW: delta 5s/30s, imbalance, buy ratio, trades/s, quote volume |
| `features/categories.py` | 10 builders puros + `build_all` + `CATEGORY_KEYS` |
| `strategies/__init__.py` | Marker del package |
| `strategies/signal_engine.py` | SignalEngine + 8 scorers puros + gating + `Signal` evaluation |
| `tests/test_feature_categories.py` | Tests de categorías, causal reproduction, time-bound |
| `tests/test_signal_engine.py` | Tests de score, components, gating, weights, no look-ahead |
| `HANDOFF_FASE3.md` | Este documento |

## 3. Archivos modificados (FASE 3)

| Archivo | Cambios |
|---|---|
| `core/models.py` | + SignalComponent, Signal, `features_by_category`, `to_dict` extendido |
| `config/strategies.py` | + thresholds, gating, validación siempre-on, `_SCORE_DIMENSIONS` |
| `config/settings.py` | + field `strategies`, `_load_strategy_config()`, env parsing |
| `features/feature_engine.py` | + categories.build_all, obi_delta, roc30/60, last_obi tracking |
| `external_data/store.py` | + `ttl_ms` property, `recent_count()` |
| `main.py` | + `--mode signal`, SignalEngine in pipeline, `_log_signal` provenance |
| `tests/test_settings.py` | + 5 tests de strategy env parsing |
| `.env.example` | + sección FASE 3 |
| `README.md` | Actualización completa |

---

## 4. Verificación hecha

- **pytest**: 103/103 en verde (68 FASE2 + 35 FASE3).
- **`--mode signal --symbols BTCUSDT`**: con datos reales → `score=62.784918`,
  `signal_type=LONG`, `eligible=true`, `regime=trending_up`, con desglose completo
  de las 8 dimensiones y sus features.
- **Causal reproduction test**: freeze de arrays a decision-time + re-computación
  pura = snapshot exacto → prueba de no look-ahead en indicadores.
- **Time-bound test**: features dependen estrictamente de `now_ms` de decisión
  (el TradeAggregator no incluye eventos futuros al instante de decisión).

---

## 5. Decisiones técnicas FASE 3

1. **Pesos NO son óptimos**: los defaults son una razonable empezar desde 0.20/0.20
   trend/momentum, 0.15 volume/orderbook, 0.10 volatility/price_structure, 0.05
   news/ml. Cualquier afirmación requiere backtesting (FASE 8).
2. **Regime gating en Signal Engine** (no en Risk Engine): el gating es de estrategia,
   no de riesgo. RiskEngine (FASE 5) mantiene la última palabra sobre límites de
   capital, drawdown, etc.
3. **scoring para SHORT**: todos los sub-scores codifican alcista en [0,100];
   valores bajos → SHORT. No hay scoring 'bearish explicit' (es simétrico por diseño).
4. **Volas por categoría vs FeatureSnapshot top-level**: ambos existen. Las categorías
   son consumidas por SignalEngine (provenance completa); el top-level se mantiene
   para compatibilidad con FASE 2 y herramientas externas.
5. **regime_name en categoría es string** (mantiene legibilidad para logging;
   `features_by_category["market_regime"]["regime_name"]` = "trending_up").

---

## 6. Limitaciones conocidas FASE 3

1. **ORDER FLOW = 0 en modo one-shot/signal** (aggTrades REST sin agresor `m`).
2. **Pesos son baseline**: requieren backtesting (FASE 8) y paper trading (FASE 7).
3. **ML = 50 siempre**: el sistema aún no consume predicciones probabilísticas;
   FASE 4 las inyecta.
4. **Las 8 dimensiones del score asumen que su suma ponderada captura la oportunidad
   de scalping**: la estructura se valida en backtesting, no se afirma a priori.
5. **Gating por régimen es binario** (en o fuera de `allowed_regimes`); refinamientos
   continuos se implementarán en fases posteriores.
6. **`ob_level` en `extra` es un entero**, no float → `to_dict()` lo omite en la
   exportación scara de `extra`; esto es aceptable (información de diagnóstico).

---

## 7. Cómo cerrar la sesión (FASE 3)

- Código subido a GitHub (rama `main`).
- Este HANDOFF_FASE3.md guardado en la raíz del proyecto.
- `README.md` y `.env.example` actualizados.
- **Detenido esperando autorización para FASE 4 (Machine Learning).**

---

## 8. Prompt para arrancar FASE 4 (ML)

> Estamos en el proyecto `C:\MCP\binancebot` (FASE 1-3 completas; 103 tests;
> pipeline produce FeatureSnapshot por símbolo + SignalEngine con 0-100 score
> y 10 feature categories timestamped).
>
> Implementa FASE 4 — "Machine Learning" — sin romper FASE 1-3:
>
> 1. Entrena modelos de clasificación probabilística (P(up)/P(down)/P(neutral))
>    que consumen exclusivamente las 10 categorías de features ya disponibles
>    en `features_by_category` del FeatureSnapshot.
> 2. ML nunca controla ejecución directamente; su función es filtro
>    probabilístico. El componente `ml` del Signal Score (dimensión weight 0.05
>    por defecto) pasa de neutro (50) a la probabilidad escalada.
> 3. Soporta walk-forward validation, time-series split, out-of-sample testing.
> 4. Compara Logistic Regression → Random Forest → Gradient Boosting (XGBoost
>    solo si justificado). Feature importance y calibration.
> 5. Evita look-ahead: walk-forward split con fechas, no shuffle random.
> 6. Modo offline (sin red) con datos históricos sintéticos + verificación
>    de que el predictor alimenta correctamente SignalEngine (test con
>    snapshot que incluye ml dimension ≠ 50).
> 7. Añade tests para: split temporal correcto, ausencia de leak, calibration,
>    y prueba end-to-end de que SignalEngine usa la predicción.
> 8. 103+ tests totales en verde. Detente y espera autorización para FASE 5.
