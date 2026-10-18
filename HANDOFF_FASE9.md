# FASE 9 — Auditoría + Testnet + operación 24/7 (HANDOFF)

Esta fase convierte el sistema de "bien diseñado sobre el papel" a **operable de
verdad**: la auditoría encontró que, aunque los 392 tests pasaban, el modo paper
**no arrancaba** y el pipeline en tiempo real **nunca procesaba un evento**. Todo
lo encontrado está corregido y fijado con tests de regresión (433 tests).

## Cómo ejecutar

```powershell
python -m crypto_scalper.main --mode paper --symbols BTCUSDT,ETHUSDT,SOLUSDT
# dashboard + API en http://localhost:8080/   (health: /healthz)

# órdenes reales en la cuenta DEMO de Binance Futures:
$env:EXECUTION_VENUE="testnet"; $env:BINANCE_TESTNET_API_KEY="..."; $env:BINANCE_TESTNET_API_SECRET="..."
python -m crypto_scalper.main --mode paper
```

Despliegue: `Dockerfile` + `render.yaml` (web service always-on en Frankfurt con
disco persistente en `/var/data`).

## Hallazgos de la auditoría y corrección

### P0 — el sistema no funcionaba de extremo a extremo
| # | Problema | Corrección |
|---|---|---|
| 1 | `run_paper` llamaba `_build_observability_repository` sin `start_equity` → `TypeError` al arrancar | pasado explícitamente; test de regresión |
| 2 | `SymbolProcessor.run()` nunca se lanzaba: colas sin consumidor | `_MarketData` arranca WS → processors → feature workers |
| 3 | El processor comparaba `isinstance(item, AggTrade)` sobre wrappers `TradeEvent/DepthEvent`: **todos los eventos se descartaban** | desenvuelve `item.trade` / `item.diff` |
| 4 | `await bus.publish_nowait(...)` (función síncrona) → `TypeError` → tormenta de reconexiones | sin `await`; overflow = drop contado |
| 5 | Sincronización del order book incorrecta para USD-M (validaba `pu` en el primer evento, `U<=id+1` siempre, no bufferizaba durante resync) → resync infinito | reglas oficiales: 1er evento `U<=id+1<=u+1`, luego `pu==u previo`; buffer durante resync |
| 6 | **Binance separó los streams**: `/stream` legacy ya no entrega `@aggTrade` | trades por `/market/stream`, depth por `/public/stream` |
| 7 | Reloj del host desfasado (31 s en la máquina de pruebas) hacía que datos frescos parecieran viejos | `core/clock.py` alineado con `serverTime`, re-sync periódico |

### P0 — seguridad de ejecución/riesgo
| # | Problema | Corrección |
|---|---|---|
| 8 | Timeout de protección dejaba la posición **sin SL/TP** y el reconciliador no la veía | timeout ⇒ abort (cancelar + cerrar a mercado) + barrido tardío de protecciones en vuelo |
| 9 | Excepciones no-`ExecutionError` (las de un adapter real) saltaban el abort | se captura cualquier excepción tras el fill |
| 10 | Reintentos podían **duplicar una entrada MARKET** (Binance solo deduplica ids de órdenes abiertas) y reintentaban rechazos | antes de reintentar se consulta el venue; `OrderRejectedError`/`Fatal` nunca se reintentan |
| 11 | SL/TP llenado durante `open()` → estado corrupto, PnL contado dos veces, símbolo bloqueado para siempre | `_finalize_close` idempotente, re-chequeo de estado, guard por símbolo antes de rutear |
| 12 | Límite de pérdida **diaria** nunca se reseteaba, ignoraba comisiones y usaba equity actual | rollover UTC, PnL neto de fees, denominador = equity de inicio de día |
| 13 | Halt por racha de pérdidas era permanente | cooldown configurable (`RISK_LOSS_STREAK_COOLDOWN_S`) + reanudar desde el dashboard |
| 14 | La **cantidad se redondeaba con el tick de precio** (0.01) → casi todo trade = 0 | cantidad solo por `stepSize`; filtros reales por símbolo desde `exchangeInfo` |
| 15 | Precios SL/TP sin redondear al tick (Binance rechaza −1111) | SL redondea alejándose de la entrada, TP hacia la entrada |
| 16 | Con ATR=0 el Risk Engine **aprobaba sin stop-loss** | rechazo `DATA_NOT_READY` |
| 17 | Cap de apalancamiento solo por trade: N posiciones = N× exposición | cap de exposición bruta de cartera (`equity × max_leverage`) |
| 18 | `notional = qty * qty` (fallback) | `qty * precio` |

### P1 — operación 24/7
- Procesos zombi: si el motor o el WS morían el proceso seguía vivo con exit 0 → supervisor con `FIRST_COMPLETED` y código ≠ 0.
- Persistencia O(N) por tick (reescribía todas las órdenes/posiciones) → incremental por firma, throttled 1 s; señales FLAT muestreadas (1/30 s/símbolo); retención de tablas.
- SQLite sin WAL/busy_timeout con lector concurrente → WAL + `busy_timeout` + índices.
- `refresh()` consultaba todas las órdenes de la historia → solo no-terminales; cachés acotadas.
- Consumer del PositionManager moría en silencio y filtraba colas → resistente, reutiliza la cola.
- Reconciliador: primer pase sin guard, cancelaba órdenes en vuelo y órdenes manuales → solo toca ids del bot (`ENTRY-/SL-/TP-/CLOSE-/FLAT-`), difiere con posiciones en transición, pausa de verdad ante fatales (antes `set_paused` nunca se conectaba).
- Símbolo suspendido nunca se recuperaba → backoff infinito con tope.
- Backoff del WS nunca se reseteaba (Binance corta cada 24 h) → reset tras conexión sana.
- Features sobre datos viejos o book desincronizado → gate de frescura y `is_synced`.
- Logs: redacción sobre la línea final (antes `extra` y tracebacks filtraban secretos), formato JSON opcional, stdout-only en contenedor.
- Rutas relativas al CWD → relativas a la raíz del proyecto.

### P2 — calidad de features
RSI Wilder real, ADX = DX suavizado (antes devolvía DX), `volume_burst_5s_ratio`
siempre 1.0, régimen con sesgo alcista, OBI sobre top-20 niveles, lookback acotado.

## Piezas nuevas

| Módulo | Rol |
|---|---|
| `execution/binance_client.py` | REST firmado HMAC, offset de tiempo, mapeo de códigos Binance → excepciones tipadas, throttling por `X-MBX-USED-WEIGHT` y `Retry-After` |
| `execution/binance_futures.py` | `BinanceFuturesAdapter`: MARKET/LIMIT en `/fapi/v1/order` (`RESULT`), **SL/TP en Algo Service** `/fapi/v1/algoOrder` (obligatorio desde 2025-12-09, `MARK_PRICE`), user-data stream + poll REST de respaldo, one-way mode, leverage, `preflight()` |
| `execution/filters.py` | `SymbolFilters`/`FilterRegistry` con `Decimal` |
| `config/venue.py` | `EXECUTION_VENUE=paper|testnet`, servidor HTTP; secretos `repr=False` |
| `trading/account.py::VenueAccount` | equity desde la wallet real del testnet |
| `monitoring/runtime.py` | estado en memoria para `/healthz` y dashboard |
| `server/app.py` + `dashboard/static/index.html` | dashboard web embebido (sin Streamlit) + API JSON + controles con token |
| `core/clock.py` | reloj alineado con el exchange |

## Decisiones

- **Datos de mercado de producción, ejecución en testnet**: el libro del testnet es
  fino/distorsionado (spread de ~0.13 % en BTC); las señales se calculan sobre el
  mercado real y los SL/TP se disparan por `MARK_PRICE` (que sigue al índice real).
  Las entradas MARKET sí llenan contra el libro del testnet: el slippage de testnet
  es peor que el real.
- **Dinero real deshabilitado a propósito**: `build_execution_stack("live")` sigue
  lanzando `NotImplementedError`; solo `paper` y `testnet` son seleccionables.
- **Keys ausentes/inválidas ⇒ paper**, visible en el dashboard, en vez de un bucle
  de reinicios.
- Streamlit eliminado: el dashboard lo sirve el propio bot (un servicio, un disco,
  estado en vivo como PnL no realizado y salud del feed).

## Pendiente / limitaciones honestas

- El adapter de testnet se probó contra un fake del API que reproduce los
  endpoints y formas de payload documentados; la primera sesión con keys reales
  es la validación definitiva (ver `/healthz` → `user_stream_connected`).
- Los pesos del Signal Score siguen **sin validar** estadísticamente: nada aquí
  afirma rentabilidad. Usar el testnet para medir antes de cualquier otra cosa.
- Sin recuperación de posiciones tras reinicio: al arrancar, el reconciliador
  aplana posiciones del bot que el proceso ya no conoce (conservador).
