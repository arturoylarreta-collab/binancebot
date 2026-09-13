# HANDOFF — cierre de sesión (FASE 5)

Fecha: 2026-09-13
Proyecto: crypto_scalper
Estado: **FASE 1 + FASE 2 + FASE 3 + FASE 4 + FASE 5 completas y testadas (229 tests en verde).**

---

## 1. Resumen de lo completado en FASE 5

### Paquete `crypto_scalper/risk/` (nuevo)
- `position_sizing.py`: `PositionSizer` con la fórmula por riesgo
  (`risk_amount = equity × risk_per_trade_pct`; `qty = risk_amount / stop_distance`),
  ajuste por cap de apalancamiento (`equity × max_leverage`), tick size y lot size
  (floor robusto con EPS de precisión), detección de equidad cero / stop distance cero.
- `stop_loss.py`: `StopLossCalculator` con modo ATR-based (`entry ± atr_mult×ATR`) y
  fixed-pct; clamped a `[0.05%, 5%]` de referencia para evitar stops absurdos.
  Degenerate inputs → `stop_price=0.0, method="invalid"`.
- `take_profit.py`: `TakeProfitCalculator` derivado del stop distance con `rr_ratio`
  (default 2.0) y suelo en `min_cost_edge_ratio` (default 1.5).
- `exposure.py`: `ExposureManager` con checks de máximo de posiciones, riesgo total
  abierto y límite por grupo correlacionado. Caps duros: `>=` es breach.
- `portfolio.py`: `PortfolioState` (dataclass frozen) con equity, posiciones abiertas,
  PnL diario, drawdown vs peak, pérdidas consecutivas, y agrupación de exposición
  por grupo de correlación / régimen. `position_sizing`... sin estado mutable interno.
- `risk_engine.py`: `RiskEngine.assess(signal, portfolio_state, ...) → RiskDecision`.
  **Autoridad final** sobre el Signal. Checks fail-fast en orden:
  1. kill switch / trading halted / safe mode → `TRADING_HALTED` / `SAFE_MODE`;
  2. elegibilidad de estrategia (`signal.eligible`);
  3. límite de pérdida diaria → `TRADING_HALTED` / `DAILY_LOSS_LIMIT`;
  4. drawdown máximo → `SAFE_MODE` / `MAX_DRAWDOWN`;
  5. protección por pérdidas consecutivas (pause_after) → `TRADING_HALTED`;
  6. max posiciones → `REJECTED` / `RISK_EXPOSURE_EXCEEDED`;
  7. riesgo total abierto → `REJECTED` / `RISK_EXPOSURE_EXCEEDED`;
  8. grupo correlacionado → `REJECTED` / `CORRELATION_EXCEEDED`;
  9. position sizing → quantity > 0 bajo cap de leverage;
  10. take-profit (`rr_ratio`) y gate de edge mínimo (habilitado, computo completo en FASE 8).
  Devuelve `RiskDecision` con `position_size`, `stop_loss_price`, `take_profit_price`,
  `notional_value`, `risk_amount`, `leverage_used` y `risk_checks` (dict por gate).
  Determinista: `now_ms` opcional inyectable (no usa reloj en hot path).

### Modelo extendido (compatible hacia atrás)
- `core/models.py`: `RiskDecision` gana campos opcionales (`position_size`,
  `stop_loss_price`, `take_profit_price`, `notional_value`, `risk_amount`,
  `leverage_used=1`, `risk_checks`). Los campos nuevos tienen default → FASE 1-4 intactas.

---

## 2. Archivos creados (FASE 5)

| Archivo | Responsabilidad |
|---|---|
| `risk/__init__.py` | Marker de paquete |
| `risk/position_sizing.py` | Sizing por riesgo (qty/notional/leverage cap/tick/lot) |
| `risk/stop_loss.py` | SL ATR-based o fijo, con min/max clamp |
| `risk/take_profit.py` | TP por risk:reward con ratio mínimo |
| `risk/exposure.py` | Max posiciones, riesgo total, grupo correlacionado |
| `risk/portfolio.py` | PortfolioState frozen (equity/PnL/drawdown/grupos) |
| `risk/risk_engine.py` | RiskEngine.assess → RiskDecision (autoridad final) |
| `tests/test_risk_position_sizing.py` | 13 tests |
| `tests/test_risk_stop_loss.py` | 10 tests |
| `tests/test_risk_take_profit.py` | 7 tests |
| `tests/test_risk_exposure.py` | 11 tests |
| `tests/test_risk_portfolio.py` | 11 tests |
| `tests/test_risk_engine.py` | 17 tests (integración + jerarquía) |

## 3. Archivos modificados (FASE 5)

| Archivo | Cambios |
|---|---|
| `core/models.py` | `RiskDecision`: + position_size/stop_loss_price/take_profit_price/notional_value/risk_amount/leverage_used/risk_checks (opcionales) |
| `README.md` | Status FASE 5, árbol `risk/`, tabla de configuración, decisiones, limitaciones |
| `pyproject.toml` | Descripción del proyecto incluye FASE 5 |

---

## 4. Verificación hecha

- **pytest**: 229/229 en verde (160 previos + 69 nuevos de FASE 5).
- **Fórmula verificada manualmente**: equity=10k, risk 1%, entry=50 000, ATR=500
  → risk_amount=100, stop=49 250, notional≈exacto a `qty×entry`, TP≈51 500 (RR 2).
- **Jerarquía**: un `Signal` elegible puede ser rechazado por exposición
  (`RISK_EXPOSURE_EXCEEDED`), por grupo correlacionado (`CORRELATION_EXCEEDED`),
  por pérdida diaria (`DAILY_LOSS_LIMIT`), por drawdown (`MAX_DRAWDOWN`) o por
  pérdidas consecutivas. El riesgo manda sobre estrategia y ML.
- **Determinista**: `assess()` con `now_ms` fijo produce decisiones byte-idénticas
  (sin red, sin reloj, sin estado mutable interno).

---

## 5. Decisiones técnicas FASE 5

1. **Función pura + estado frozen**: `RiskEngine` no guarda estado interno; el
   snapshot de la cuenta se pasa como `PortfolioState` (dataclass frozen). Esto hace
   el componente 100% offline, testeable y pseudo-conectores para FASE 6-7 (el
   Execution/Position manager actualizará el estado tras fills o PnL).
2. **Fail-fast con razón tipada**: cada gate rechaza con su `RejectReason` y un
   `risk_checks` dict con el resultado de todos los gates evaluados hasta ahí →
   trazabilidad completa de la decisión.
3. **RiskDecision extendido sin romper nada**: campos opcionales con default; los
   que consumen `RiskDecision` en FASE 1-4 no cambian.
4. **Caps duros son `>=`**: alcanzar exactamente el cap de riesgo total o de grupo
   es breach (no tolerancia). Consistente con "límites duros".
5. **Leverage como cap de notional, no multiplicador ciego**: `max_leverage` limita
   `notional ≤ equity × max_leverage`; `leverage_used` reporta el cap configurado.
   El sizing sigue siendo por riesgo y puede resultar 1x aunque el config permita 3x.
6. **SL/TP no negociables por la estrategia**: el RiskEngine los calcula y los
   entrega en la decisión; la secuencia `Entry → Fill → Protección` obligatoria
   se impondrá en FASE 6.
7. **Gate de edge mínimo habilitado pero sin cómputo de costes**: es un placeholder
   honesto (check `minimum_edge=True`); el modelo de costes real (fees+spread+
   slippage+funding) es FASE 8.

---

## 6. Limitaciones conocidas FASE 5

1. **No actualiza el estado de cuenta por sí solo**: `assess()` consume un snapshot
   `PortfolioState`; quien lo construye (paper/FASE 7 o execution/FASE 6) debe
   reflejar fills, SL/TP tocados y PnL realizado. No hay mutación dentro del RiskEngine.
2. **SL usa ATR por defecto; necesita atr** en la llamada. Si `atr=0`, el SL queda
   `invalid` y el sizing devuelve quantity 0 → REJECTED. Es deliberado (nunca aprobar
   sin SL válido), pero el llamador debe alimentar ATR del snapshot.
3. **El gate de edge mínimo no computa costes** (FASE 8); el `min_cost_edge_ratio`
   ya existe en `RiskConfig` y se usa como suelo del ratio de TP.
4. **Grupos de correlación estáticos** (config; default BTC/ETH/SOL): correlaciones
   dinámicas entre activos quedan como mejora futura (FASE 9 optimización).
5. **Tradeoff de orden de checks**: el límite total (3%) es más estricto que el de
   grupo (6%) por defecto; en la mayoría de configs el grupo correlacionado nunca se
   alcanza antes que el total. Es correcto (el total es la restricción dominante),
   pero si se quiere testear el grupo hay que relajar el total (más de 6%).

---

## 7. Cómo cerrar la sesión (FASE 5)

- 229/229 tests en verde con `python -m pytest -q`.
- `python -m crypto_scalper.ml_train --seconds 180` sigue siendo green (regresión FASE 4).
- Este `HANDOFF_FASE5.md` en la raíz del proyecto.
- `README.md` y `pyproject.toml` actualizados.
- **Detenido esperando autorización para FASE 6 (Execution Engine).**

---

## 8. Prompt para arrancar FASE 6 (Execution Engine)

> Estamos en `C:\MCP\binancebot` (FASE 1-5 completas; 229 tests; pipeline con
> FeatureSnapshot → SignalEngine 0-100 con dimensión `ml` alimentada por un
> MLPredictor calibrado y validado offline, y RiskEngine como autoridad final que
> entrega RiskDecision con position_size / stop_loss / take_profit / risk_checks).
>
> Implementa FASE 6 — "Execution Engine" — sin romper FASE 1-5:
>
> 1. `ExchangeAdapter` (abstracción) + `SimulatedExecutionAdapter` (sin red) como
>    implementación default; la interfaz debe permitir después Binance Futures.
> 2. `OrderManager`: soporta MARKET/LIMIT con idempotencia (client_order_id),
>    retry con backoff, timeout, cancel/replace y detección de órdenes duplicadas.
> 3. `PositionManager`: secuencia obligatoria e irreversible
>    `Signal → Risk approval → Entry → Fill → Protección → Position active`; nunca
>    permite ENTRY FILLED sin STOP LOSS + TAKE PROFIT confirmados.
> 4. `ExecutionRouter`: decide adapter según modo (paper/simulado por ahora).
> 5. Peticiones de `RiskEngine.assess()` → si APPROVED, ejecuta y registra la posición;
>    si REJECTED/SAFE_MODE/TRADING_HALTED, nunca envía orden.
> 6. Tests offline: orden, fill, protecciones, duplicados, timeout, retry, jerarquía
>    risk > execution, y la regla "nunca sin SL/TP".
> 7. 229+ tests totales en verde. Detente y espera autorización para FASE 7.