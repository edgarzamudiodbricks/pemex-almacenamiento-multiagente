# Guía de despliegue

Requisitos: workspace con Unity Catalog, serverless, Lakebase Autoscaling y Databricks Apps; permiso para crear esquemas en `<CATALOG>`.

## 0. Clonar y parametrizar

1. Clona el repo como Git folder (Workspace > Create > Git folder).
2. Reemplaza los marcadores de la tabla del [README](../README.md#parámetros). Busca `<CATALOG>` y el resto en todo el repo.

## 1. Esquemas y datos sintéticos

```sql
CREATE SCHEMA IF NOT EXISTS <CATALOG>.pemex_alm_bronze;
CREATE SCHEMA IF NOT EXISTS <CATALOG>.pemex_alm_silver;
CREATE SCHEMA IF NOT EXISTS <CATALOG>.pemex_alm_gold;
CREATE SCHEMA IF NOT EXISTS <CATALOG>.pemex_alm_ml;
CREATE VOLUME IF NOT EXISTS <CATALOG>.pemex_alm_bronze.landing;
```

Ejecuta `01_datos_sinteticos/01_generador_datos_sinteticos_almacenamiento` con `modo = backfill`. La última celda valida el % de llenado por tipo de instalación, producto y año.

## 2. Pipeline

Crea el pipeline con `config/pipeline_medallion.json` (UI de pipelines o `POST /api/2.0/pipelines`) y ejecútalo. Anota su ID como `<PIPELINE_ID>`.

Validación: `<CATALOG>.pemex_alm_gold.serie_forecast_instalacion_producto` debe tener 242 series sin huecos.

## 3. Modelo y endpoint

1. Ejecuta `03_ml/02_modelo_pronostico_capacidad`. Registra el modelo con alias `champion` y escribe `pronostico_capacidad_diario` y `alertas_capacidad`; la última celda valida rangos y orden de cuantiles.
2. Crea el endpoint con `config/serving_endpoint.json`, usando la versión `champion` como `<MODEL_VERSION>`.
3. Ejecuta `03_ml/03_actualizar_endpoint_pronostico`: deja el endpoint en `champion` y hace un smoke test contra la tabla.

## 4. Genie Agent y dashboard

* Genie: sigue `07_genie/README.md`. Anota el ID como `<GENIE_SPACE_ID>`.
* Dashboard: importa `06_dashboard/pemex_almacenamiento.dashboard.json` (Dashboards > Create > Import dashboard from file; renómbralo con extensión `.lvdash.json` si la UI lo pide) y asigna el warehouse `<WAREHOUSE_ID>`.

## 5. Lakebase y App

1. Ejecuta `04_lakebase/04_setup_lakebase`: crea el proyecto `pemex-almacenamiento-chat` (PostgreSQL 17) y prueba la conexión. **No crees el esquema `pemex_chat` a mano**: lo crea la App con su service principal.
2. Crea la App `pemex-almacenamiento-chat` y agrega los recursos de `config/app_resources.json` (Genie `CAN_RUN`, endpoint `CAN_QUERY`, warehouse `CAN_USE`, Lakebase `CAN_CONNECT_AND_CREATE`). Los nombres de recurso deben coincidir con los `valueFrom` de `05_app_chat/app.yaml`.
3. Da permisos de lectura al service principal de la App (cambio de permisos; solo lectura):

```sql
GRANT USE CATALOG ON CATALOG <CATALOG> TO `<APP_SP_CLIENT_ID>`;
GRANT USE SCHEMA, SELECT ON SCHEMA <CATALOG>.pemex_alm_gold TO `<APP_SP_CLIENT_ID>`;
GRANT USE SCHEMA, SELECT ON SCHEMA <CATALOG>.pemex_alm_ml TO `<APP_SP_CLIENT_ID>`;
```

4. Despliega desde `05_app_chat/` (`databricks apps deploy pemex-almacenamiento-chat --source-code-path <REPO_PATH>/05_app_chat`).

## 6. Job diario

Crea el Job con `config/job_ingesta_medallion.json` (`POST /api/2.2/jobs/create`), sustituyendo `<REPO_PATH>`, `<PIPELINE_ID>` y `<USER_EMAIL>`.

## Pruebas de punta a punta

1. **Smoke test de arranque:** pon `RUN_SMOKE_TEST: "true"` en `app.yaml`, despliega y busca en los logs `INIT_OK` (con `owner` = service principal), `SMOKE_OK ruta=genie` y `SMOKE_OK ruta=modelo`. Vuelve a ponerlo en `"false"`.
2. **En la UI de la App:**
   * "¿Cuáles son las 5 instalaciones con mayor porcentaje de llenado actual?" -> Ruta: Genie Agent, con tabla y SQL.
   * "¿Qué pasa si la capacidad operativa de la TAD Santa Catarina (Monterrey) para diésel baja a 80%?" -> Ruta: Modelo, con base vs escenario p10/p50/p90.
   * "¿y si baja a 50%?" -> reutiliza la misma serie.
3. **En Lakebase:** `pemex_chat.conversaciones` y `pemex_chat.mensajes` deben tener tu usuario, la ruta y la latencia de cada respuesta, sin errores.
4. **Consistencia del modelo:** el p50 guardado para un escenario debe coincidir con una llamada directa al endpoint con la misma `fecha` y `frac_capacidad_operativa`.
