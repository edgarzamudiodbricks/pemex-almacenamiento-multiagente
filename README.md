# PEMEX Almacenamiento: inventarios, pronóstico de capacidad y chat multiagente en Databricks

Demo de punta a punta sobre Databricks para analizar el producto almacenado (crudos, gasolinas, diésel, turbosina, combustóleo y gas LP) en refinerías, terminales marítimas y TAD, y pronosticar el llenado de la capacidad de almacenamiento a 30 días.

> **Datos sintéticos.** Las instalaciones y su ubicación se basan en información pública; capacidades, inventarios, recibos y despachos son generados. No representan datos operativos reales de PEMEX.

## Componentes

| Carpeta | Componente | Tecnología |
| --- | --- | --- |
| `01_datos_sinteticos/` | Generador de catálogos y telemetría diaria por tanque (backfill e incremental, idempotente) | Notebook PySpark |
| `02_pipeline_medallion/` | Pipeline Bronze -> Silver -> Gold con expectativas, cuarentena y Auto CDC | Lakeflow Spark Declarative Pipelines |
| `03_ml/` | Modelo de pronóstico (LightGBM global, p10/p50/p90, horizonte 30 d), actualización del endpoint y plan de acción por alerta redactado por IA generativa | MLflow, Unity Catalog, Model Serving, AI Functions (`ai_query`) |
| `04_lakebase/` | Proyecto Postgres para el historial del chat | Lakebase Autoscaling |
| `05_app_chat/` | Chat que rutea preguntas de datos a Genie y escenarios what-if al modelo, más la pestaña "Plan de acción" para aprobar, modificar o rechazar recomendaciones | Databricks Apps (Streamlit) |
| `06_dashboard/` | Dashboard: estado actual, alertas y pronóstico, tendencia mensual | AI/BI Dashboards |
| `07_genie/` | Instrucciones, tablas y SQL de ejemplo del Genie Agent | Genie |
| `config/` | Definiciones del Job, el pipeline, el endpoint y los recursos de la App | JSON (REST API) |
| `docs/` | Arquitectura y guía de despliegue | Markdown |

## Parámetros

Los valores propios del workspace original se reemplazaron por marcadores. Sustitúyelos antes de desplegar:

| Marcador | Significado | Dónde aparece |
| --- | --- | --- |
| `<CATALOG>` | Catálogo de Unity Catalog con los esquemas `pemex_alm_*` | Notebooks, pipeline, App, dashboard, Genie, `config/` |
| `<WAREHOUSE_ID>` | SQL warehouse para Genie, dashboard y consultas de la App | `05_app_chat/config.py`, `config/app_resources.json`, `07_genie/` |
| `<GENIE_SPACE_ID>` | ID del Genie Agent creado con `07_genie/README.md` | `05_app_chat/config.py`, `04_lakebase/`, `config/app_resources.json` |
| `<LAKEBASE_HOST>` | Host del endpoint Lakebase (solo desarrollo local; en la App lo inyecta el recurso) | `05_app_chat/config.py` |
| `<USER_EMAIL>` | Tu usuario del workspace | Experimento MLflow en `03_ml/`, notificaciones del Job |
| `<REPO_PATH>` | Ruta del Git folder, p. ej. `/Workspace/Users/<USER_EMAIL>/pemex-almacenamiento-multiagente` | `config/job_ingesta_medallion.json`, `config/pipeline_medallion.json` |
| `<PIPELINE_ID>` | ID del pipeline creado con `config/pipeline_medallion.json` | `config/job_ingesta_medallion.json` |
| `<MODEL_VERSION>` | Versión del modelo con alias `champion` | `config/serving_endpoint.json` |

No hay secretos en el repositorio: la App usa la identidad OAuth de su service principal y Lakebase usa tokens OAuth de corta duración.

## Esquemas en Unity Catalog

* `pemex_alm_bronze`: volume `landing` (CSV de catálogos y JSON de telemetría) y tablas crudas.
* `pemex_alm_silver`: dimensiones, mantenimientos, telemetría depurada (Auto CDC SCD1), cuarentena y hecho diario por tanque sin huecos.
* `pemex_alm_gold`: inventario diario por instalación-producto, snapshots actuales (instalación y tanque), resumen mensual y serie para pronóstico.
* `pemex_alm_ml`: modelo `modelo_pronostico_capacidad`, `pronostico_capacidad_diario`, `alertas_capacidad` y `plan_accion_alertas` (acción recomendada por alerta, con el contexto enviado al LLM y el resultado de las validaciones).

## Inicio rápido

Sigue [docs/despliegue.md](docs/despliegue.md). Resumen del orden:

1. Crear esquemas y el volume `landing`; ejecutar el generador en modo `backfill`.
2. Crear y ejecutar el pipeline.
3. Ejecutar el notebook del modelo y crear el endpoint.
4. Crear el Genie Agent y el dashboard.
5. Crear Lakebase, la App y sus recursos; desplegar.
6. Crear el Job diario.

## Limitaciones conocidas

* El efecto de `frac_capacidad_operativa` en el modelo es débil y puede ser contraintuitivo (reducir capacidad baja ligeramente el % pronosticado). Además la respuesta es escalonada: valores cercanos (p. ej. 70% y 80%) pueden dar el mismo resultado. La App lo advierte en cada escenario.
* El endpoint no admite filas repetidas `(serie_id, fecha)` en un mismo request; la App envía base y escenario en llamadas separadas.
* El endpoint tiene scale-to-zero: la primera llamada puede tardar más de 60 s; la App reintenta.
* El router del chat es determinista (expresiones regulares); preguntas ambiguas se pueden forzar con el selector de ruteo.

Más detalle en [docs/arquitectura.md](docs/arquitectura.md).
