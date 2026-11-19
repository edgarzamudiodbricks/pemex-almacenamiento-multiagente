# Modelo de pronóstico y endpoint

## Backtest (holdout de 30 días por serie)

Fuente: experimento MLflow, run `modelo_final_refit` del 2026-11-19 12:06 UTC (reentrenamiento diario del Job).

| Modelo | MASE | MAE | RMSE | sMAPE |
| --- | --- | --- | --- | --- |
| LightGBM global (p50) | **1.455** | 2.560 | 3.346 | 4.164 |
| Naive (último valor) | 2.991 | 5.394 | 6.768 | 8.822 |
| Seasonal naive (7 d) | 3.223 | 5.807 | 7.082 | 9.487 |

* El MASE de LightGBM es 51% menor que el del naive (1.455 contra 2.991).
* Cobertura del intervalo p10-p90 en el holdout: 67.6% (nominal 80%; el intervalo es algo estrecho).

## Smoke test del endpoint `modelo_pronostico_capacidad`

Fuente: Model Serving API. Estado `READY`, versión servida 11, la misma que generó la tabla de pronóstico. Se envían filas de `pronostico_capacidad_diario` y se compara el p50 devuelto:

| serie_id | fecha | p50 tabla | p50 endpoint | diferencia |
| --- | --- | --- | --- | --- |
| REF-CAD\|COMB | 2026-11-19 | 88.28 | 88.2764 | 0.0036 |
| REF-CAD\|COMB | 2026-12-03 | 88.24 | 88.2397 | 0.0003 |
| REF-CAD\|COMB | 2026-12-18 | 85.88 | 85.8785 | 0.0015 |
| REF-CAD\|CRU-ISTMO | 2026-11-19 | 43.45 | 43.4490 | 0.0010 |
| REF-CAD\|CRU-ISTMO | 2026-12-03 | 42.76 | 42.7640 | 0.0040 |
| REF-CAD\|CRU-ISTMO | 2026-12-18 | 43.40 | 43.3971 | 0.0029 |

Diferencia máxima 0.004 puntos porcentuales: la tabla guarda el p50 redondeado a 2 decimales.
