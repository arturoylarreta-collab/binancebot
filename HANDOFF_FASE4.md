# HANDOFF — cierre de sesión (FASE 4)

Fecha: 2026-09-13
Proyecto: crypto_scalper
Estado: **FASE 1 + FASE 2 + FASE 3 + FASE 4 completas y testadas (160 tests en verde).**

---

## 1. Resumen de lo completado en FASE 4

### Paquete `crypto_scalper/ml/` (nuevo)
- `features.py`: `ValueSchema` (columnas `"categoria.key"` ordenadas y
  deterministas, strings/excluidos, NaN→0), `forward_return_labels` (etiquetas
  down/neutral/up por horizonte+umbral, O(n) estricto, tail enmascarada) y
  `build_dataset` → `LabeledDataset` ordenado por tiempo con `class_counts()`.
  Vectoriza **solo `categories.CATEGORY_KEYS`** (10 categorías base), nunca la
  categoría `ml` → no hay feedback loop.
- `walkforward.py`: `WalkForwardSplit` anclado por tiempo (sin shuffle),
  garantía estricta `max(ts_train) + horizon_ms <= min(ts_test)`, folds
  contiguos que cubren el rango de test, `calibration_split()` y
  `final_holdout_split()` temporales, métricas `multiclass_brier`, log-loss y
  ECE en `evaluate()`.
- `classifier.py`: fábricas LR (dentro de `Pipeline(scaler, clf)`), RF, GB;
  `fit_calibrated` con `CalibratedClassifierCV(method="isotonic", cv=K)` **dentro
  del fold de entrenamiento** (el test nunca se toca); **degradación elegante**:
  si la clase minoritaria no permite validación K-fold (folds pequeños), devuelve
  el estimador sin calibrar en vez de crashear. `predict_proba` alinea `classes_`
  al grid fijo `[down, neutral, up]` (fold sin una clase → columna 0.0).
  `feature_importance` desenvuelve el Pipeline del LR para leer `coef_`.
- `confidence.py`: `confidence_from_proba` puro → `ml_score` (0-100, 50 neutro),
  `edge`, `confidence`, `uncertainty`, `dominant_class`. `ml_score = k + k·edge`.
- `predictor.py`: `MLPredictor` (modelo + ValueSchema + parámetros de etiquetado),
  `.predict(cats)` → `MLPrediction` (provenance completa) y `.category(cats)` →
  dict inyectable en `features_by_category["ml"]`. Serialización joblib
  (`save`/`load` con corrupción rechazada).
- `model_manager.py`: `ModelManager.evaluate_candidates` compara modelos
  fold-a-fold por Brier; `fit_best` selecciona el mejor, entrena final
  (calibrado) en train menos holdout **temporal de cola** y devuelve
  `MLPredictor` + `SelectionReport` (OyS metrics, top features, versionado
  `<modelo>-v1`).
- `synthetic.py`: generador offline determinista (episodios `up/down/range`
  desfasados por símbolo, drift multiplicativo) con `predictor` opcional para
  regenerar snapshots con categoría `ml` inyectada.

### Integración (sin romper FASE 3)
- `features/feature_engine.py`: parámetro opcional `predictor` en `FeatureEngine`;
  tras `build_all(...)` inyecta `features_by_category["ml"] = predictor.category(...)`.
- `strategies/signal_engine.py`: `_score_ml` ahora lee
  `cat.get("ml_score", 50.0)`; sin predictor → 50 (contrato FASE 3 intacto).
- `requirements.txt` / `pyproject.toml`: `scikit-learn>=1.9`, `joblib>=1.3`
  (sklearn 1.9 eliminó `multi_class=` y `cv="prefit"`).

### Tests nuevos (57)
`tests/test_ml_features.py` (9): determinismo del schema/orden, relleno de
columnas faltantes, exclusión de strings/NaN, forward-return manual
(down/neutral/up + tail), dataset ordenado, **filas = freeze del snapshot en su
`ts` (no future features)**, reproducibilidad, entrada vacía rechazada.

`tests/test_ml_walkforward.py` (9): cobertura contigua y disjunta de folds,
**gap ≥ horizonte en cada fold** (`is_strict`), train monótonamente creciente,
sin shuffle (determinista), rechazo de ventanas demasiado pequeñas, split cal
cronológico y disjunto, holdout final con gap, Brier a mano, ECE en [0,1],
métricas de modelo perfecto.

`tests/test_ml_classifier.py` (6): contrato de probabilidades (3 columnas,
suma 1) para LR/RF/GB, alineación cuando el training no tiene `neutral`
(columna 0.0), **degradación en fold minúsculo**, single-class rechazado,
importances caps y descenso.

`tests/test_ml_confidence.py` (9): score bounds, edge/confidence/uncertainty,
normalización, clipping, determinismo, monotonicidad.

`tests/test_ml_predictor.py` (5): campos de `MLPrediction`, keys de `category`,
categoría ausente → columnas 0, roundtrip joblib vía `tmp_path`, objetos
ajenos rechazados.

`tests/test_ml_model_manager.py` (5): selección por Brier sobre walk-forward
con folds reales, holdout de cola nunca visto, esquema del artefacto
(model_version ≥ v1, schema == columnas del dataset), **reproducibilidad por
seed**, ranking de LR/RF/GB.

`tests/test_ml_integration.py` (6 + 1 `slow`): stub determinista a través de
FeatureEngine y SignalEngine (`ml` ≠ 50, score total afectado, bounded),
fallback sin predictor (regresión FASE 3) y end-to-end real con modelo
entrenado (`SyntheticMarket` con `predictor` → snapshot con `ml` → Signal).

---

## 2. Archivos creados (FASE 4)

| Archivo | Responsabilidad |
|---|---|
| `ml/__init__.py` | Marker de paquete |
| `ml/features.py` | ValueSchema, forward_return_labels, build_dataset, LabeledDataset |
| `ml/walkforward.py` | Split temporal anclado + métricas (Brier/log-loss/ECE) |
| `ml/classifier.py` | Fábricas LR(scaled)/RF/GB, calibración isotónica, predict_proba alineada |
| `ml/confidence.py` | Mapeo prob→ml_score (componente ml del Signal) |
| `ml/predictor.py` | MLPredictor (artefacto joblib) + MLPrediction |
| `ml/model_manager.py` | Comparación/Selección por Brier + SelectionReport |
| `ml/synthetic.py` | SyntheticMarket (offline determinista) |
| `ml_train.py` | Runner: gen → dataset → walk-forward → selección → e2e Signal |
| `tests/test_ml_*.py` (7 archivos) | 57 tests de FASE 4 |

## 3. Archivos modificados (FASE 4)

| Archivo | Cambios |
|---|---|
| `features/feature_engine.py` | `predictor` opcional; inyección de categoría `ml` |
| `strategies/signal_engine.py` | `_score_ml` lee `ml_score` real (fallback 50) |
| `requirements.txt` / `pyproject.toml` | + scikit-learn, joblib; marker `slow` |
| `README.md` | Status FASE 4, `ml/` en árbol, comando `ml_train`, limitaciones |
| `7 tests/test_ml_*.py` | ver tabla de tests |

---

## 4. Verificación hecha

- **pytest**: 160/160 en verde (23 FASE1 + 45 FASE2 + 35 FASE3 + 57 FASE4).
- **`python -m crypto_scalper.ml_train --seconds 180`** (4 símbolos, seed
  fija): 712 snapshots → 592 muestras / 80 features; walk-forward 3 folds →
  RF brier=0.44 / GB 0.46 / LR 0.74 → **best=random_forest**, OOS holdout de
  cola: accuracy 0.76, brier 0.29, existing log_loss 0.51; top features ATR/
  regime/EMAs/order-book; end-to-end: snapshot con `ml` (p_up=0.94,
  ml_score=93.8) y Signal con `ml_component=93.85`.
- La selección por Brier elige honestamente el mejor (LR, mal calibrado en
  folds pequeños, queda en último lugar y no contamina la elección).

---

## 5. Decisiones técnicas FASE 4

1. **ML = filtro probabilístico, nunca controla ejecución**: entra como una
   dimensión más (0-100) del Signal Score; Risk Engine (FASE 5) es autoridad.
2. **Calibración dentro del fold**: isotonic + K-fold CV en el training fold;
   el test del walk-forward y el holdout final nunca tocan el empaque de
   calibración (no leakage).
3. **Grid de clases fijo [down, neutral, up]**: si el fold no tiene una clase,
   `predict_proba` devuelve 0.0 en esa columna, manteniendo el contrato de la
   dimensión `ml`.
4. **Sin pandas en hot path**: vectorización NumPy de `features_by_category`
   (80 columnas); la categoría `ml` se excluye del vector para evitar feedback.
5. **sklearn 1.9**: LR usa `Pipeline(StandardScaler, LogisticRegression)`
   (converge sin `ConvergenceWarning` y sin `multi_class=`); la calibración usa
   `CalibratedClassifierCV(cv=K)` (ya no existe `cv="prefit"`).
6. **Degradación elegante en folds pequeños**: minoría <4 ejemplos → estimador
   sin calibrar en vez de excepción; la comparación sigue siendo honesta porque
   el Brier lo penaliza.
7. **Synthetic offline determinista**: episodios de régimen desfasados por
   símbolo (phase = index%3) con drift multiplicativo → las 3 clases aparecen en
   cualquier ventana temporal; determinismo total por seed (merges de test y
   reproduce del reporte).
8. **Walk-forward anclado**: gap `>= horizon` entre el último label del train y
   el primer test de cada fold; partición sin shuffle; `is_strict()` lo verifica
   de forma comprobable en tests.

---

## 6. Limitaciones conocidas FASE 4

1. **No es backtesting**: métricas OOS sobre datos sintéticos son plausibles,
   no rentabilidad (FASE 8 es backtest, FASE 7 paper trading).
2. **Logistic es vulnerable a calibración isotónica en folds pequeños** → Brier
  /log-loss altos; la comparación lo muestra y selecciona RF/GB. Mejora futura:
   sigmoid (`method="sigmoid"`) en datasets grandes o `cross_val` con más folds.
3. **Neutral (clase 1) es minoritaria** (~5-10%): el walk-forward exige
   suficiente span por símbolo (demo usa 180 s > 4×horizonte); con span menor el
   fold 0 queda vacío o degenerado.
4. **Sidebar**: `joblib` emite un `DeprecationWarning` de NumPy 2.5 en roundtrip
   (proveniente de la propia librería, benigno, no es código de proyecto).
5. El adaptador a producción (train con datos reales + reciclaje del artefacto
   en `main.py`) y el refresh por reentrenamiento quedan para fases posteriores.

---

## 7. Cómo cerrar la sesión (FASE 4)

- 160/160 tests en verde con `python -m pytest -q`.
- `python -m crypto_scalper.ml_train --seconds 180` verificado end-to-end.
- Este `HANDOFF_FASE4.md` en la raíz del proyecto.
- `README.md`, `requirements.txt` y `pyproject.toml` actualizados.
- **Detenido esperando autorización para FASE 5 (Risk Engine).**

---

## 8. Prompt para arrancar FASE 5 (Risk Engine)

> Estamos en `C:\MCP\binancebot` (FASE 1-4 completas; 160 tests; pipeline con
> FeatureSnapshot + SignalEngine 0-100 con dimensión ml alimentada por un
> MLPredictor calibrado y validado offline).
>
> Implementa FASE 5 — "Risk Engine" — sin romper FASE 1-4:
>
> 1. Un componente que consume `RiskConfig` (caps duros ya existentes: riesgo
>    por operación 1%, apalancamiento máx 3x, pérdida diaria 3%, max drawdown
>    10%) y decide autorizar/bloquear operaciones sobre el `Signal` evaluado.
> 2. Valida el trade propuesto contra: exposición/cap por operación, límite de
>    pérdida diaria, drawdown máximo, tamaño de posición (fórmula por riesgo),
>    y estado de cuenta simulado (paper).
> 3. Da autoridad final: un `Signal` elegible por la estrategia aún puede ser
>    rechazado por riesgo (reason tipado). El riesgo nunca es solo gating de
>    estrategia.
> 4. Modo offline determinista: `RiskEngine.assess(Signal) -> RiskDecision`, sin
>    red y con tests de las 4 comprobaciones.
> 5. Añade tests de cada cap duro y de jerarquía (riesgo > estrategia).
> 6. 160+ tests totales (no romper los 160) en verde. Detente y espera
>    autorización para FASE 6.