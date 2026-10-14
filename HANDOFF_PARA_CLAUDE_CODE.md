# Handoff de Binancebot para Claude Code

Fecha de preparación: 14 de octubre de 2026 (America/Caracas).

## 1. Carpeta de origen y estado de Git

Carpeta de origen del trabajo (puedes usar otro directorio al clonar):

```text
C:\Users\Jose Arturo\Documents\ChatGPT\binancebot
```

- Repositorio remoto: https://github.com/arturoylarreta-collab/binancebot
- Rama de continuación: `codex/dashboard-interactivo`.
- Commit base local: `7addbca8eebec24d77098e90a43bbd75467e42f4`.
- Mensaje: `FASE 5-8.5: risk engine, execution, paper trading, backtesting, observability, and SQLite audit trail`.
- **Los cambios descritos forman parte del commit que incluye este handoff**, preparado para publicación en `origin/codex/dashboard-interactivo` por petición del usuario. Para continuar desde GitHub, utiliza esa rama, no `main`.
- Si usas la carpeta original, conserva cualquier cambio local posterior: revisa `git status` antes de actualizar. No hagas `git reset --hard` ni sustituyas esa carpeta por un clon.
- El documento inicial del usuario está en `C:\Users\Jose Arturo\Downloads\prompt_para_bot_de_binance.md`. Es una referencia de requisitos, no código fuente ni autorización para ampliar el alcance.

Para obtener el trabajo en una carpeta nueva:

```powershell
git clone --branch codex/dashboard-interactivo --single-branch https://github.com/arturoylarreta-collab/binancebot.git
Set-Location -LiteralPath binancebot
```

La `.venv`, las bases SQLite y el documento de Downloads son archivos locales y no se incluyen en GitHub. Los requisitos y pendientes necesarios para continuar están recogidos aquí y en el README.

## 2. Objetivo vigente y restricciones

El encargo de desarrollo era convertir el dashboard en una herramienta multipágina de análisis interactivo para dos socios. El usuario solicitó preparar este handoff y publicar el código y la documentación en `codex/dashboard-interactivo` para continuar desde GitHub; no se ha autorizado una fase nueva de trading real.

Requisitos del dashboard:

1. Resumen más historial de trades, señales/rechazos, riesgo/exposición y logs.
2. Plotly, auto-refresco cada cinco segundos y consultas cacheadas cuatro segundos.
3. Filtros globales de fechas, símbolos y modo `backtest`, `paper`, `live`.
4. Equity/drawdown con dos ejes, PnL acumulado y distribución de señales.
5. Tablas interactivas, KPIs y textos propios de la aplicación en español.
6. Lectura exclusiva de SQLite, proceso separado y mínimo impacto sobre el bot.

**No modificar RiskEngine ni ExecutionEngine.** El alcance de implementación está en `crypto_scalper/dashboard/`, `crypto_scalper/monitoring/dashboard_data.py`, `pyproject.toml` y la documentación solicitada. Los tests nuevos se guardaron dentro de `dashboard/tests/` para respetar ese límite.

No activar trading real ni usar API keys para verificar el dashboard.

## 3. Contexto del bot existente

El paquete Python es `crypto_scalper`, versión declarada `0.8.1`, con Python mínimo `3.11`.

El código base ya contiene ingesta de mercado asíncrona, features/señales, filtro ML, riesgo, ejecución simulada, paper trading, backtesting y observabilidad con SQLite/Telegram. Para contexto histórico, leer `README.md` y los archivos `HANDOFF_FASE2.md` a `HANDOFF_FASE85.md`.

Rutas relevantes:

- `crypto_scalper/main.py`: entrada y selección de modo.
- `crypto_scalper/risk/`: motor de riesgo y sus componentes.
- `crypto_scalper/execution/`: contratos, simulador, órdenes, posiciones y reconciliación.
- `crypto_scalper/trading/`: orquestador, cuenta y reconciliador.
- `crypto_scalper/backtest/`: datos, venue, motor y reportes.
- `crypto_scalper/storage/sqlite_repo.py`: esquema real y persistencia.
- `crypto_scalper/monitoring/observability.py`: snapshots y observación del repositorio.

**LIVE no está implementado en este checkout**: `main.py` devuelve error para ese modo y no hay un adaptador de ejecución live. La opción `live` del dashboard es un filtro de datos; no habilita operaciones reales. No interpretar menciones a «FASE 9c» como evidencia de que existe una integración live terminada.

## 4. Cambios incluidos respecto al commit base

```text
binancebot/
  README.md                              modificado: uso, semántica y WAL
  pyproject.toml                         modificado: dependencias y testpaths
  crypto_scalper/
    monitoring/dashboard_data.py         reescrito: consultas de solo lectura
    dashboard/
      app.py                             modificado: Resumen y navegación
      common.py                          nuevo: caché, filtros y presentación
      charts.py                          nuevo: gráficos Plotly y locale español
      pages/
        1_Historial_Trades.py             nuevo
        2_Señales_y_Rechazos.py            nuevo
        3_Riesgo_y_Exposición.py           nuevo
        4_Logs_del_Sistema.py             nuevo
      tests/
        conftest.py                      nuevo: SQLite con esquema del bot
        test_queries.py                  nuevo: filtros y agregaciones
        test_app.py                      nuevo: AppTest, caché y navegación
```

Detalles importantes:

- `DashboardFilters` es un dataclass inmutable con límites UTC, símbolos y modo.
- SQL parametrizado, URI `mode=ro`, `PRAGMA query_only`, timeout de bloqueo de 50 ms y presupuesto de 250 ms por consulta mediante progress handler.
- Se cierran cursores y conexiones; no se comparten conexiones entre sesiones ni se mantiene una transacción durante toda la página.
- La caché vive en `dashboard/common.py`, con `@st.cache_data(ttl=4)` y entradas acotadas. La capa de consultas no importa Streamlit ni motores.
- Se corrigió la exposición cero cuando todavía no había operaciones cerradas.
- Los agregados usan todo el intervalo. Las tablas muestran hasta 5.000 filas; las curvas se muestrean conservando primer/último punto, después de calcular el acumulado.
- `app.py` registra las páginas con `st.navigation`, renderiza los filtros comunes y crea `st_autorefresh(interval=5_000)`. Hay un interruptor de pausa.
- `require_entrypoint()` resuelve la entrada inicial por la navegación antigua que Streamlit puede detectar al existir `pages/`. Redirige por `app.py`, conserva los widgets y restaura la página solicitada.
- No se han modificado los motores, el escritor SQLite ni el esquema de persistencia del bot.

## 5. Semántica que no debe inventarse

El esquema existente no permite cumplir algunos filtros con desglose exacto:

- **Heartbeats:** incluyen modo, pero capital/drawdown son de toda la cartera; no contienen símbolo. La interfaz avisa cuando el usuario filtra símbolos.
- **Modo en posiciones/señales/eventos:** no existe en el esquema actual. Un filtro específico solo incluye filas sin modo si los heartbeats de toda la base identifican un único modo coincidente. En bases mezcladas o sin esa información, esas tablas quedan vacías con aviso. «Todos» permite consultarlas sin atribuir modo. No asociar registros al heartbeat temporal más cercano ni inventar una procedencia.
- **Trades cerrados:** filtrar `status='CLOSED'` y motivo no nulo ni vacío. `close_reason` tiene como valor predeterminado `''`, por lo que `IS NOT NULL` por sí solo incluiría posiciones abiertas.
- **Precio de salida:** se obtiene de la orden persistida correspondiente al ID de SL, TP o `CLOSE-<POSITION_ID_EN_MAYÚSCULAS>`. Si falta, es NULL; no se fabrica a partir del PnL.
- **PnL:** se acumula desde cero dentro del período filtrado; es el PnL bruto de `positions`, antes de comisiones/financiación.
- **Exposición:** representa posiciones actualmente abiertas cuya apertura cae dentro del filtro, no una reconstrucción histórica de la cartera.
- **Señales:** una señal elegible puede tener rechazo posterior. El gráfico considera aprobada una LONG/SHORT elegible sin rechazo registrado, sin afirmar que se ejecutó una orden.
- **Logs:** son eventos, decisiones de riesgo y reconciliaciones guardados en SQLite; no se leen archivos `.log` ni payloads completos.
- **Fechas:** UTC, inicio inclusivo y final exclusivo a medianoche del día siguiente. El rango elegido no se amplía automáticamente al cambiar de día.

### Condición de concurrencia: WAL

El dashboard no cambia el journal de SQLite. El escritor actual tampoco activa WAL automáticamente. Con journal `delete`, las lecturas pueden bloquear brevemente las escrituras aunque el dashboard use una conexión de solo lectura.

Para evitar interferencia de lectores se requiere que el propietario prepare la DB en WAL, con el bot detenido. El README incluye el comando administrativo. **No afirmar que el requisito estricto de cero bloqueo está garantizado en cualquier base o por usar `mode=ro`.** No modificar el escritor ni migrar la base por iniciativa propia dentro de este alcance.

## 6. Entorno local y comandos

Se comprobó el entorno existente al preparar este documento:

- `.venv\Scripts\python.exe`: Python `3.14.5`.
- Streamlit `1.64.0`, Plotly `6.9.0`, streamlit-autorefresh `1.0.1`, pytest `9.1.1`.
- La `.venv` tiene `include-system-site-packages = true`; hereda paquetes globales y **no es un entorno aislado reproducible**. En la sesión de desarrollo anterior Streamlit era `1.60.0`. Revisar compatibilidad si aparecen cambios de comportamiento.
- `.venv/` está excluida localmente mediante `.git/info/exclude`.
- En la carpeta original existe una DB **sintética de prueba**, no de operaciones reales, en `.venv/dashboard-preview/sesión #1.db`. No está versionada ni disponible en un clon nuevo.
- No se ha entregado ni verificado una base real de producción para este dashboard.

Desde PowerShell:

```powershell
Set-Location -LiteralPath 'C:\Users\Jose Arturo\Documents\ChatGPT\binancebot'
git status --short --branch

# Verificación focalizada, offline:
.\.venv\Scripts\python.exe -m pytest crypto_scalper/dashboard/tests tests/test_dashboard_data.py tests/test_monitoring.py -q

# Regresión sin el test marcado slow:
.\.venv\Scripts\python.exe -m pytest -q -m 'not slow'

# Preview con datos sintéticos existentes:
$env:DASHBOARD_DB_PATH = (Resolve-Path -LiteralPath '.venv/dashboard-preview/sesión #1.db').Path
.\.venv\Scripts\python.exe -m streamlit run crypto_scalper/dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false
```

Para una instalación limpia desde la rama de GitHub:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

# Generar una DB sintética con la misma fixture usada en los tests.
# Solo se crea si ese archivo de prueba aún no existe.
.\.venv\Scripts\python.exe -c "from pathlib import Path; from crypto_scalper.dashboard.tests.conftest import database; p=Path('logs/dashboard-preview'); p.mkdir(parents=True, exist_ok=True); database.__wrapped__(p) if not (p/'sesión #1.db').exists() else None"
$env:DASHBOARD_DB_PATH = (Resolve-Path -LiteralPath 'logs/dashboard-preview/sesión #1.db').Path
.\.venv\Scripts\python.exe -m streamlit run crypto_scalper/dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false
```

Para datos del bot, exportar `DASHBOARD_DB_PATH` con la ruta de su SQLite existente. El valor predeterminado es `logs/paper.db`; el dashboard no carga las API keys del `.env` ni crea la base.

Antes de arrancar un servidor, verificar si el puerto está ocupado y qué proceso lo usa. En la sesión anterior hubo un preview de este trabajo en 8501 y otro proceso ajeno en 8502; esos datos son históricos, no estado actual. No detener procesos ajenos ni reutilizar PIDs antiguos.

## 7. Verificaciones y pendientes reales

### Evidencia de la sesión anterior

- `test_app.py`: **7 passed**, después de corregir el arranque de enlaces directos y la conservación de filtros.
- Consultas nuevas más tests existentes de dashboard/monitoring: **26 passed**.
- Resto de tests existentes con `-m 'not slow'`, excluyendo los dos archivos anteriores: **378 passed, 1 deselected**; warnings preexistentes de joblib/NumPy.
- Total histórico de esas ejecuciones: **411 tests aprobados**, repartidos en varias invocaciones; no es una ejecución única de toda la suite ni incluye el test slow.
- `pip check` y `git diff --check` pasaron en la sesión anterior. `git diff --check` se volvió a comprobar al preparar este handoff, sin errores de whitespace; Git advierte sobre conversión LF/CRLF.

### Verificación al preparar este handoff

El 14 de octubre de 2026 se ejecutó de nuevo:

```powershell
.\.venv\Scripts\python.exe -m pytest crypto_scalper/dashboard/tests tests/test_dashboard_data.py tests/test_monitoring.py -q
```

Resultado: **33 passed in 34.94s**, con el Streamlit `1.64.0` instalado actualmente.
No se volvió a ejecutar toda la suite ni se reinició el preview durante la
preparación del handoff. Este resultado confirma los tests focalizados, pero no
resuelve por sí mismo el pendiente visual descrito a continuación.

### Pendiente principal: aceptación visual después de reinicio limpio

Se vio en navegador el Resumen, los gráficos, el filtro BTCUSDT, la pausa por teclado y la navegación al historial con el filtro conservado. Después de añadir `require_entrypoint` y recargar el servidor que ya estaba abierto, la **última comprobación visual terminó con este error**:

```text
AttributeError: module 'crypto_scalper.dashboard.common' has no attribute 'require_entrypoint'
```

La función sí existe en el archivo actual y sus tests habían pasado. La hipótesis es un módulo antiguo retenido en el proceso de desarrollo, pero **no se confirmó** con un reinicio limpio. No dar la validación visual por cerrada ni presentar el dashboard como certificado para producción.

Próximos pasos recomendados al continuar:

1. Leer este documento, `README.md` y el diff respecto al commit base, preservando cualquier cambio local posterior.
2. Revisar el resultado de las pruebas actuales y reproducir cualquier fallo antes de modificar código.
3. Lanzar un proceso nuevo de Streamlit contra la DB sintética, en un puerto libre, y comprobar que `common.require_entrypoint` corresponde al módulo de esta carpeta.
4. Validar las cinco páginas y los enlaces directos con servidor recién iniciado; comprobar filtros, pausa/refresco, tablas y recuperación ante DB ausente/ocupada.
5. Revisar detalles visuales: algunos textos nativos de Streamlit (menús, ayudas, pantalla completa) pueden seguir en inglés; la última tabla mostraba `None` para salida desconocida aunque la leyenda decía «vacío». La última modificación de las cuatro tarjetas KPI tampoco recibió aceptación visual completa.
6. Comprobar rendimiento con una base representativa: no se ha realizado una prueba de carga grande ni demostrado ausencia de interferencia con un bot real. Los límites de tiempo protegen al lector, pero no reemplazan WAL ni una evaluación de consultas sobre históricos grandes.
7. Documentar cualquier corrección restante y ejecutar las pruebas pertinentes. Mantener cambios fuera de riesgo/ejecución.

El código y este handoff se entregan en la rama de continuación para revisión. **Publicar el commit no cierra los pendientes de aceptación visual ni certifica el dashboard para producción.**

## 8. Prompt sugerido para Claude Code

```text
Usa la rama codex/dashboard-interactivo del repositorio
https://github.com/arturoylarreta-collab/binancebot.
La carpeta de origen es C:\Users\Jose Arturo\Documents\ChatGPT\binancebot;
si trabajas en otro equipo, clona esa rama y crea un entorno virtual nuevo.
Lee HANDOFF_PARA_CLAUDE_CODE.md y revisa el estado de Git antes de cambiar nada.
Conserva cualquier cambio local posterior al commit del handoff.
Retoma la validación pendiente del dashboard multipágina, empezando por un proceso
limpio de Streamlit y las pruebas offline. No modifiques RiskEngine ni ExecutionEngine,
no actives trading real y no amplíes el esquema de persistencia sin acordar ese alcance.
Explica qué verificaste, qué corregiste y qué limitaciones siguen abiertas.
```
