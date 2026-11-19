# Evidencia de ejecución

Salidas reales del build corriendo en Databricks, extraídas el 19-nov-2026 de las APIs de Jobs, Pipelines, MLflow y Model Serving, de las tablas de Unity Catalog, de Lakebase y de los logs de la App.

* Los datos son **sintéticos** (ver `01_datos_sinteticos/`).
* Se reemplazaron los valores propios del workspace: correo del usuario por `<USER_EMAIL>` y client ID del service principal de la App por `<APP_SP_CLIENT_ID>`. No se incluyen IDs de Job, pipeline, warehouse ni workspace.
* Horas en UTC salvo que se indique otra zona. El Job corre a las 06:00 America/Mexico_City (12:00 UTC).

| Archivo | Qué demuestra |
| --- | --- |
| [01_job_corridas.md](01_job_corridas.md) | 11 corridas consecutivas del Job, todas `SUCCESS`, incluida la tarea de plan de acción con IA generativa |
| [02_calidad_datos.md](02_calidad_datos.md) | Conteos Bronze/Silver/Gold, deduplicación, cuarentena por regla y relleno de huecos, con su conciliación |
| [03_modelo_endpoint.md](03_modelo_endpoint.md) | Métricas de backtest contra baselines y smoke test del endpoint contra la tabla de pronóstico |
| [04_plan_accion_ia.md](04_plan_accion_ia.md) | Recomendaciones generadas por el LLM, validaciones en código y su ejecución dentro del Job |
| [05_app_lakebase_logs.md](05_app_lakebase_logs.md) | Logs de la App (arranque, Genie, modelo, decisiones) y lo que quedó registrado en Lakebase |
