# Handoff de Binancebot para Claude Code

Fecha de preparación: 14 de octubre de 2026 (America/Caracas).
Actualizado el 14 de octubre de 2026 por la sesión de Claude Code que completó
la validación visual (ver secciones 7 y 8). Los resultados de la sesión
anterior se conservan como historial.

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
- Commit publicado de referencia: `7df145536f68c854ae84272e89b74f233e0054ef` (`feat(dashboard): add interactive multipage analysis and Claude handoff`). La sesión de validación partió de ese commit con el árbol limpio. Sus correcciones se confirmaron y publicaron en `origin/codex/dashboard-interactivo` en el commit siguiente, que incluye esta versión del handoff, por petición del usuario.
- Al hacer `git fetch` apareció una rama remota nueva, `origin/fase-9-testnet`. No se revisó ni se integró.
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
- `preserve_widgets()` (sesión de validación) guarda filtros y pausa cuando una ejecución termina antes de renderizar la barra lateral: redirección, base ausente o lectura bloqueada. Solo se llama si la barra no se renderizó, para no sobrescribir una selección más reciente. Los valores predeterminados de los widgets viven en Session State, no en sus argumentos, para que restaurarlos no provoque el aviso de conflicto de Streamlit.
- `number()`/`percent()` y `styled()` aplican separadores españoles a KPIs y tablas sin convertir las celdas en texto (la ordenación sigue siendo numérica).
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
- La sesión de validación generó otra con el comando de abajo en `logs/dashboard-preview/sesión #1.db`, y una copia de prueba en `logs/dashboard-preview/prueba-estados.db` (journal `delete`) para simular base ausente, vacía y bloqueada. `logs/` está en `.gitignore`.
- No se ha entregado ni verificado una base real de producción para este dashboard.
- `pyproject.toml` exige ahora `streamlit>=1.64` (versión validada; se usan `delta_arrow` y tamaños de `st.metric`) y `jinja2>=3.1` (formato de tablas con `Styler`). `requirements.txt` no incluye Streamlit desde antes de este trabajo y queda fuera del alcance autorizado.

Desde PowerShell:

```powershell
Set-Location -LiteralPath 'C:\Users\Jose Arturo\Documents\ChatGPT\binancebot'
git status --short --branch

# Verificación focalizada, offline:
.\.venv\Scripts\python.exe -m pytest crypto_scalper/dashboard/tests tests/test_dashboard_data.py tests/test_monitoring.py -q

# Regresión sin el test marcado slow:
.\.venv\Scripts\python.exe -m pytest -q -m 'not slow'

# Preview con datos sintéticos (proceso nuevo, sin recarga en caliente):
$env:DASHBOARD_DB_PATH = (Resolve-Path -LiteralPath 'logs/dashboard-preview/sesión #1.db').Path
.\.venv\Scripts\python.exe -m streamlit run crypto_scalper/dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --server.fileWatcherType none --client.toolbarMode viewer --browser.gatherUsageStats false
```

Con `--server.fileWatcherType none`, **reinicia el proceso tras cada cambio de código**: no recarga módulos (ver 7.2).

Para una instalación limpia desde la rama de GitHub:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

# Generar una DB sintética con la misma fixture usada en los tests.
# Solo se crea si ese archivo de prueba aún no existe.
.\.venv\Scripts\python.exe -c "from pathlib import Path; from crypto_scalper.dashboard.tests.conftest import database; p=Path('logs/dashboard-preview'); p.mkdir(parents=True, exist_ok=True); database.__wrapped__(p) if not (p/'sesión #1.db').exists() else None"
$env:DASHBOARD_DB_PATH = (Resolve-Path -LiteralPath 'logs/dashboard-preview/sesión #1.db').Path
.\.venv\Scripts\python.exe -m streamlit run crypto_scalper/dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --server.fileWatcherType none --client.toolbarMode viewer --browser.gatherUsageStats false
```

El comando de generación se ejecutó el 14 de octubre de 2026 con pytest 9.1.1 y creó la base correctamente (`database.__wrapped__` sigue disponible en esa versión).

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

### Pendiente de la sesión anterior (cerrado en 7.1–7.5)

La última comprobación visual previa terminó con
`AttributeError: module 'crypto_scalper.dashboard.common' has no attribute 'require_entrypoint'`
después de editar el código con el servidor abierto. La causa no estaba confirmada.

### 7.1 Validación visual del 14 de octubre de 2026 (Claude Code)

Entorno: navegador integrado de Claude Desktop (Chromium), servidores Streamlit
nuevos en `127.0.0.1:8501` (base sintética `logs/dashboard-preview/sesión #1.db`)
y `127.0.0.1:8502` (base de prueba `prueba-estados.db`). Antes de arrancar se
comprobó que 8500–8520 estaban libres. Solo se detuvieron procesos lanzados en
esta sesión. Sin API keys, sin llamadas a Binance y sin bases reales.

Verificado en navegador con el código final (proceso reiniciado tras la última edición):

| Comprobación | Resultado |
|---|---|
| Enlace directo `/Historial_Trades` como **primera** petición a un servidor recién iniciado | Carga sin error, filtros por defecto |
| Enlaces directos a Señales, Riesgo y Logs en sesiones nuevas; URL inexistente | Cargan sin error; la inexistente muestra el diálogo nativo «Page not found» (inglés) y abre el Resumen |
| Navegación por las cinco páginas y recarga F5 | Sin errores; tras F5 Streamlit reanuda la sesión de la pestaña y conserva filtros |
| Filtros de fechas, símbolos (BTCUSDT/ETHUSDT) y modo (paper/live/Todos) | Aplicados en todas las páginas y conservados al navegar. «live» en esta base muestra el aviso de modo ambiguo y estados vacíos |
| Auto-refresco cada 5 s, pausa y reanudación | La marca «Lectura» avanza cada 5 s, queda fija >10 s en pausa y vuelve a avanzar al reanudar |
| Gráficos de equity/drawdown (dos ejes), PnL acumulado y distribución de señales | Correctos; hover y barra de Plotly en español |
| Tablas: ordenación por columna | Ascendente/descendente, numérica (−5 < 10 < 25) y conservada tras un refresco |
| Cuatro KPI | Formato `1.020,00` / `2,00 %`, color según signo, sin cortes a 1440, 800 y 375 px |
| Base ausente | Aviso, sin crear el archivo; al restaurarla se recupera con los filtros intactos |
| Base ocupada (`BEGIN EXCLUSIVE` en una copia con journal `delete`) | Aviso de reintento; al liberar se recupera con los filtros intactos |
| Base vacía (SQLite sin tablas) | Estados vacíos en español, sin excepciones |
| Ventana estrecha (800 px) y móvil (375 px) | KPI en 2×2 o en columna, tablas con desplazamiento interno, sin desbordamiento horizontal de la página ni solapes |

### 7.2 Qué fallaba y qué se corrigió

1. **`AttributeError ... require_entrypoint` (causa confirmada: estado del proceso, no del código).** Con un proceso limpio no se reproduce. Se reprodujo la misma clase de fallo editando los módulos con dos servidores abiertos: tras los mismos cambios, 8502 recargó `charts.py`, mientras que 8501 siguió sirviendo el `charts.py` anterior **incluso a sesiones nuevas**, ejecutando a la vez `app.py` y `common.py` nuevos. Las páginas se ejecutan desde su archivo actual, pero los módulos auxiliares viven en `sys.modules` del proceso. Por eso una página nueva puede llamar a una función que el módulo retenido no tiene. No se aisló la carrera interna del vigilante de Streamlit en Windows. Medida: arrancar con `--server.fileWatcherType none` y reiniciar tras cada cambio (README y sección 6). Capturas: `antes/09` y `antes/10`.
2. **Filtros perdidos ante una base bloqueada o ausente (bug real, reproducido en navegador).** Si la ejecución terminaba antes de renderizar la barra lateral, Streamlit descartaba el estado de sus widgets: los símbolos y el modo volvían a «Todos» y una pausa se reanudaba. Corregido con `preserve_widgets()`, sin sobrescribir selecciones nuevas. Hay un test de regresión que falla con el `app.py` anterior (`[] == ['BTCUSDT']`).
3. **KPI:** etiquetas cortadas por debajo de unos 900 px; delta de Drawdown que repetía el valor y marcaba en rojo «-0.00%»; formato inglés. Ahora se reparten en una fila que se ajusta al ancho, con separadores españoles. Drawdown muestra el máximo del período (y se eliminó la métrica duplicada de Riesgo), Acierto el PnL y Exposición las posiciones abiertas. Un cambio nulo aparece en gris. En Historial, el delta de PnL repetía el valor y ahora muestra la media por trade.
4. **Precio de salida desconocido:** Streamlit 1.64 pinta toda celda nula con el texto fijo «None» (constante del frontend, sin opción para cambiarlo). Convertirlo en «—» exigiría una columna de texto y la ordenación de precios sería alfabética. Se mantiene el valor nulo y numérico, y la leyenda dice exactamente lo que se ve: «la celda queda sin valor (la tabla la muestra como «None»)». No se infiere ningún precio.
5. **Formato y gráficos:** separadores españoles en tablas mediante `Styler`, sin perder la ordenación numérica. «1 fila mostrada» en singular. Eje de drawdown con marcas limpias (antes `1,973 / 1,605…`). Conteos de señales enteros. Leyendas en orden. LONG/SHORT visible en exposición. Fechas «22 sep» y hover `dd/mm/aaaa`. Leyenda anclada sobre el área del gráfico para que no tape los ejes ni la barra táctil en móvil.
6. **Interfaz:** placeholder «Todos» en Modo. Con `--client.toolbarMode viewer` desaparece el botón «Deploy».

### 7.3 Pruebas y comprobaciones ejecutadas

- Focalizadas: `pytest crypto_scalper/dashboard/tests tests/test_dashboard_data.py tests/test_monitoring.py -q` → **35 passed** (33 anteriores + 2 nuevas: conservación de filtros ante base bloqueada/ausente y formato/valor nulo numérico).
- Suite sin `slow`: `pytest -q -m "not slow"` → **413 passed, 1 deselected** en una única ejecución (7 warnings preexistentes de joblib/NumPy). El test `slow` no se ejecutó.
- `pip check` → sin dependencias rotas. `git diff --check` → sin errores (solo avisos LF/CRLF de Git).

### 7.4 Capturas

Carpeta local, ignorada por Git (no se publica en GitHub):
`logs/dashboard-validacion/capturas/`

- `antes/`: los defectos encontrados (`None` y formato inglés, eje de drawdown, fracciones, KPI cortadas, barra lateral ausente con la base ocupada, filtro perdido tras el bloqueo, leyenda solapada en móvil, módulo retenido en 8501 frente a 8502).
- `final/`: Resumen, gráficos, Historial con filtros y orden numérico, Señales, Riesgo, Logs, estado vacío, 800 px, móvil, base ocupada y ausente con los filtros conservados.
- `final/04`, `05`, `07`, `09`, `10`, `11`, `12` y `15` se tomaron antes del último ajuste de la leyenda de Plotly. Las páginas sin gráfico no cambian; `final/01–03`, `06`, `08`, `13` y `14` reflejan el código final.

### 7.5 Limitaciones y pendientes reales

- Siguen en inglés controles nativos de Streamlit: menú principal, «Select all», «Clear value», barra de la tabla (buscar, descargar CSV, columnas), «Fullscreen», «Page not found», atributos de accesibilidad del calendario y «None» en celdas nulas. Streamlit no expone una traducción.
- Las capturas usan datos sintéticos de la fixture (un solo día y marcas de milisegundos). No se ha revisado la legibilidad con históricos largos ni realizado una prueba de carga. Los presupuestos de 250 ms protegen al lector, pero no sustituyen una evaluación con datos representativos.
- No hay validación de concurrencia con un bot real escribiendo. La condición WAL se mantiene: `mode=ro` no garantiza ausencia de bloqueo con journal `delete`.
- Las limitaciones del esquema de la sección 5 siguen vigentes: modo no atribuible a posiciones/señales, equity solo de cartera, precio de salida solo si existe la orden y PnL bruto.
- Con tarjetas sin delta (estado vacío), la de Exposición es algo más alta que las demás. Es un detalle estético menor.
- El dashboard **no está certificado para producción**.

## 8. Prompt sugerido para Claude Code

```text
Usa la rama codex/dashboard-interactivo del repositorio
https://github.com/arturoylarreta-collab/binancebot.
La carpeta de origen es C:\Users\Jose Arturo\Documents\ChatGPT\binancebot;
si trabajas en otro equipo, clona esa rama y crea un entorno virtual nuevo.
Lee HANDOFF_PARA_CLAUDE_CODE.md y revisa el estado de Git antes de cambiar nada.
Conserva cualquier cambio local posterior al commit del handoff.
La validación visual con datos sintéticos está registrada en la sección 7.
Continúa con los pendientes reales de 7.5 que el usuario priorice (por ejemplo,
revisión con una base representativa o con el bot en paper y WAL). Arranca
Streamlit siempre como proceso nuevo con --server.fileWatcherType none y
reinícialo tras cada cambio. No modifiques RiskEngine ni ExecutionEngine,
no actives trading real y no amplíes el esquema de persistencia sin acordar ese alcance.
Explica qué verificaste, qué corregiste y qué limitaciones siguen abiertas.
```
