# Plan de acción por alerta con IA generativa

Notebook `03_ml/04_plan_accion_alertas`: `ai_query` con `system.ai.claude-sonnet-4-6`, salida estructurada y temperatura 0, más validaciones en código. Fuente: tabla `pemex_alm_ml.plan_accion_alertas`.

| Fecha de corte | Generado (UTC) | Origen | Recomendaciones | Errores del LLM | Fallan validación | ALTA | MEDIA |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-17 | 2026-11-19 01:59 | Notebook (desarrollo) | 9 | 0 | 0 | 4 | 5 |
| 2026-11-18 | 2026-11-19 12:08 | Tarea 5 del Job | 9 | 0 | 0 | 5 | 4 |

La segunda fila la generó la corrida programada del Job (ver `01_job_corridas.md`), sin intervención manual.

## Iteración durante el desarrollo

Problemas encontrados y resueltos con evidencia antes de llegar a 0 errores:

1. `ai_query` acepta un solo campo de primer nivel en `responseFormat`, pero devuelve los campos planos: se pide `STRUCT<plan:...>` y se lee el esquema interno.
2. Con `failOnError => false` el resultado es `STRUCT<result, errorMessage>`.
3. En la primera versión el modelo omitió `volumen_sugerido_bbl` en 6 de 9 respuestas. Se hizo obligatorio en las instrucciones y las 9 pasaron la validación.
4. El modelo no siempre respetaba la regla de prioridad (4 alertas de saturación y solo 3 ALTA). La prioridad se calcula ahora en código y ya no depende del LLM.

## Validaciones en código sobre cada respuesta

* La instalación alternativa debe estar en la lista enviada al LLM (misma región y producto, con capacidad libre), o ser `NINGUNA`.
* El volumen sugerido no puede exceder la capacidad libre de la alternativa.
* Se guarda el contexto exacto enviado al LLM (`contexto_json`) para trazabilidad.

## Ejemplos de recomendaciones (corte 2026-11-17)

| Serie | Prioridad | Alternativa | Volumen sugerido |
| --- | --- | --- | --- |
| REF-CAD\|COMB | ALTA | NINGUNA | 0 bbl |
| TMR-LZC\|COMB | ALTA | TMR-ACA | 8,152 bbl |
| TMR-PRO\|COMB | ALTA | REF-OLM | 3,642 bbl |
| REF-MIN\|COMB | MEDIA | TMR-TUX | 49,921 bbl |

Texto completo de la recomendación para REF-MIN: "Transferir 49,921 bbl de Combustóleo a Terminal Marítima Tuxpan (TMR-TUX) antes del 14-dic-2026, coordinando despachos adicionales para mantener el llenado por debajo del 90% ante el escenario p90 proyectado al 17-dic."
