# Corridas del Job "PEMEX Almacenamiento - Ingesta diaria y Medallion"

Fuente: Jobs API (`runs/list` con tareas). Programación diaria 06:00 America/Mexico_City.

| Inicio (UTC) | Disparo | Resultado | Duración | Tareas |
| --- | --- | --- | --- | --- |
| 2026-11-19 12:00 | PERIODIC | SUCCESS | 13.7 min | 5 |
| 2026-11-18 12:00 | PERIODIC | SUCCESS | 16.8 min | 4 |
| 2026-11-17 12:00 | PERIODIC | SUCCESS | 20.9 min | 4 |
| 2026-11-16 12:00 | PERIODIC | SUCCESS | 18.6 min | 4 |
| 2026-11-15 12:00 | PERIODIC | SUCCESS | 16.4 min | 4 |
| 2026-11-14 12:00 | PERIODIC | SUCCESS | 16.0 min | 4 |
| 2026-11-13 12:00 | PERIODIC | SUCCESS | 17.1 min | 4 |
| 2026-11-12 12:00 | PERIODIC | SUCCESS | 17.8 min | 4 |
| 2026-11-11 12:00 | PERIODIC | SUCCESS | 7.6 min | 3 |
| 2026-11-10 12:00 | PERIODIC | SUCCESS | 3.0 min | 2 |
| 2026-11-09 20:54 | ONE_TIME | SUCCESS | 2.2 min | 2 |

El número de tareas crece porque el Job se fue ampliando: ingesta + pipeline (9-nov), reentrenamiento (11-nov), actualización del endpoint (12-nov) y plan de acción con IA generativa (19-nov).

## Detalle de la corrida 2026-11-19 (5 tareas)

| Tarea | Resultado | Duración |
| --- | --- | --- |
| generar_datos_incremental | SUCCESS | 0.9 min |
| refresh_pipeline_medallion | SUCCESS | 2.5 min |
| reentrenar_y_pronosticar | SUCCESS | 4.2 min |
| actualizar_endpoint | SUCCESS | 6.1 min |
| plan_accion_alertas | SUCCESS | 0.9 min |

## Actualizaciones del pipeline (Lakeflow Spark Declarative Pipelines)

Fuente: Pipelines API (`list_updates`). Las 5 más recientes: `COMPLETED`, sin full refresh, disparadas por la tarea del Job (`JOB_TASK`), del 2026-11-15 al 2026-11-19 a las 12:00-12:01 UTC.
