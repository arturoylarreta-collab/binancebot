# FASE 8 — Backtesting (HANDOFF)

Backtest determinista y offline que reutiliza la cadena real
(Data → Features → Señal → ML → Riesgo → Ejecución → Posiciones) cambiando
solo la venue a una bar-driven histórica (`BarVenue`). Sin look-ahead y sin
doble contabilidad de costes.

## Ejecutar

```powershell
python -m crypto_scalper.main --mode backtest --trades 100
python -m crypto_scalper.main --mode backtest --start 2026-08-01 --end 2026-08-31 --trades 50
```

- `--mode backtest`: permite `--start`/`--end` (ISO UTC), `--trades` (cap).
- Corpus: se usa cache CSV en `BACKTEST_DATA_DIR` (default `data/klines/`)
  cuando cubre la ventana; si no, descarga paginada vía REST y la persiste.
- Estrategias: `main.run_backtest` fuerza `enabled=True`
  (`dataclasses.replace`), independiente de `STRATEGY_ENABLED` de live/paper.
- HL: `backend=backtest|-live` etc. VER: `crypto_scalper/main.py::run_backtest`.

## Piezas nuevas

| Módulo | Rol |
|---|---|
| `config/backtest.py` | `CostModelConfig` + `BacktestConfig` (validación en `__post_init__`) |
| `risk/cost_model.py` | `CostBreakdown`, `CostModel` (fees/spread/latencia/funding atribuidos; slippage embebido en fills), `estimate_p_win` (ML `p_up`/`p_down` → score [0,100]→[0.25,0.75]) |
| `backtest/data.py` | `kline_to_trades` (determinista O→H→L→C), `synthetic_orderbook`, CSV save/load, `KlineCorpus.events()` interleaved, `BinanceKlinesDownloader` paginado |
| `backtest/venue.py` | `BarVenue`: `process_bar` stop-first (SL(0) < TP(1) < LIMIT(2)), slippage en market, LIMIT sin slippage, `entry_fill_fraction`, clamp `reduce_only`, clock histórico, implements `ExchangeAdapter` |
| `backtest/engine.py` | `BacktestEngine`: la cadena real es la autoridad; `max_trades` cap; `_maybe_open` vía `TradeOrchestrator.on_signal` (nunca `router.route` directo) |
| `backtest/reports.py` | `TradeRecord`, `EquityPoint`, `EdgeBlock`, `BacktestReport` (`to_dict`/`summary_text`, stats por símbolo/reason) |

## Cambios a módulos previos

- `execution/execution_router.py`: kwargs opcionales `cost_model`/`enforce_edge_gate`
  (default OFF → live/paper intactos). Error `edge_below_required` + métrica
  `execution.rejected_by_edge`.
- `market_data/rest.py`: `klines(start_time, end_time)` backward-compatible.
- `config/settings.py` + `.env.example`: variables `BACKTEST_*` y `COST_*`.
- `pyproject.toml` → 0.8.0.
- `main.py`: dispatch `--mode backtest`, `run_backtest`, helpers corpus.

## Decisiones de diseño

- Costes: `fee`/`spread`/`latency`/`funding` se atribuyen sobre el notional medio
  del roundtrip; `slippage_pct` ya está en los fill prices de la venue
  (solo informativo en `CostBreakdown`, pero cuenta en el gate: `2×slippage`).
- Edge gate: `E[R] = P(win)·RR − (1−P(win))`, edge% sobre notional; entra
  (`edge − cost`) ≥ `minimum_required_edge_pct`.
- `fill_at="close"` (default) rutea al close de la barra de decisión;
  `"next_open"` almacena `(signal, snapshot)` y rutea al open siguiente
  (`Market` no puede quedar resting: `PositionManager.open` exige fill síncrono).
- Warmup: las decisiones empiezan cuando `state.candles.count >= warmup_bars`
  (cuenta candles 1s sintéticos ≈ barras de kline × steps).
- PaperAccount con `fee_pct=0.0` en backtest (fees las cobra `CostModel`).

## Tests

`tests/test_backtest_{data,costs,venue,engine}.py` — 41 casos nuevos,
total `379 passed`. Sin red (descargador sin conexión cubierto por tests de
parseo/CSV).

## Verificado

- `python -m pytest -q` → 379 passed, 7 warnings (joblib/NumPy 2.5, preexistente).
- `py_compile` de módulos tocados + import de `main` OK.

## Limbo / notas

- `max_trades` corta el replay al abrirse ese número de entradas; posiciones
  abiertas al cortar no entran en `closed_positions`.
- `EntryBlock` asume holding 0h para funding del reporte (atribución real
  consultar `edge_blocks`).
- Sharpe/PF/DD se calculan sobre la curva de equity de marcas 1s.
- Siguiente natural: reportes comparativos `by_parameter` y pruebas con el
  predictor ML (`BACKTEST_MODEL_PATH`).