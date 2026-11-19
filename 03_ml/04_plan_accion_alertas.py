# Databricks notebook source
# DBTITLE 1,Descripción
# MAGIC %md
# MAGIC # Plan de acción por alerta (IA generativa)
# MAGIC
# MAGIC Tarea 5 del Job. Para cada alerta vigente de `alertas_capacidad` (saturación, riesgo de saturación o posible desabasto) arma un contexto con:
# MAGIC
# MAGIC * la alerta y el estado actual (llenado, capacidad libre, autonomía, mantenimiento),
# MAGIC * el pronóstico p10/p50/p90 en días clave del horizonte,
# MAGIC * hasta 3 instalaciones **de la misma región y producto** con capacidad libre (alternativas reales, no inventadas).
# MAGIC
# MAGIC Un LLM (`ai_query`, salida estructurada) redacta la acción logística recomendada. Después se **valida en código** que la alternativa exista en el contexto y que el volumen sugerido no exceda su capacidad libre.
# MAGIC
# MAGIC * **Destino:** `pemex_alm_ml.plan_accion_alertas`, una fila por `(fecha_corte, serie_id)`.
# MAGIC * **Actualización:** `REPLACE WHERE fecha_corte = <corte>`; reejecutar el mismo día es idempotente y conserva el histórico de días anteriores.
# MAGIC * **Errores:** `failOnError => false`; las filas con error quedan registradas en `error_llm` y no detienen la tarea, salvo que fallen todas.

# COMMAND ----------

# DBTITLE 1,Configuración
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
ML, GOLD = f"{CATALOG}.pemex_alm_ml", f"{CATALOG}.pemex_alm_gold"
DESTINO = f"{ML}.plan_accion_alertas"
LLM = "system.ai.claude-sonnet-4-6"
HORIZONTES = (1, 3, 7, 14, 21, 30)

corte = spark.sql(f"SELECT max(fecha_corte) AS f FROM {ML}.alertas_capacidad").first()["f"]
print("fecha_corte:", corte, "| modelo:", LLM)

# COMMAND ----------

# DBTITLE 1,Contexto por alerta
contexto = spark.sql(f"""
WITH alertas AS (
  SELECT * FROM {ML}.alertas_capacidad
  WHERE fecha_corte = DATE'{corte}'
    AND (nivel_alerta <> 'NORMAL' OR fecha_posible_desabasto_p10 IS NOT NULL)
),
snap AS (SELECT * FROM {GOLD}.snapshot_instalacion_actual),
pron AS (
  SELECT serie_id,
         array_sort(collect_list(named_struct(
           'fecha', CAST(fecha AS STRING), 'p10', round(pct_llenado_p10, 1),
           'p50', round(pct_llenado_p50, 1), 'p90', round(pct_llenado_p90, 1)))) AS pronostico
  FROM {ML}.pronostico_capacidad_diario
  WHERE fecha_corte = DATE'{corte}' AND horizonte_dias IN {HORIZONTES}
  GROUP BY serie_id
),
alternativas AS (
  SELECT a.serie_id,
         slice(array_sort(collect_list(named_struct(
           'orden', -s.capacidad_disponible_bbl, 'id_instalacion', s.id_instalacion,
           'nombre_instalacion', s.nombre_instalacion, 'pct_llenado', round(s.pct_llenado, 1),
           'capacidad_disponible_bbl', round(s.capacidad_disponible_bbl, 0)))), 1, 3) AS alternativas
  FROM alertas a
  JOIN snap s ON s.id_producto = a.id_producto AND s.region = a.region
             AND s.id_instalacion <> a.id_instalacion AND s.pct_llenado < 80 AND s.capacidad_disponible_bbl > 0
  GROUP BY a.serie_id
)
SELECT a.fecha_corte, a.serie_id, a.id_instalacion, a.nombre_instalacion, a.id_producto, a.nombre_producto,
       a.region, a.nivel_alerta,
       to_json(named_struct(
         'instalacion', a.nombre_instalacion, 'tipo', a.tipo_instalacion, 'region', a.region, 'producto', a.nombre_producto,
         'nivel_alerta', a.nivel_alerta, 'fecha_corte', CAST(a.fecha_corte AS STRING),
         'pct_llenado_actual', round(a.pct_llenado_actual, 1),
         'fecha_primera_saturacion_p50', CAST(a.fecha_primera_saturacion AS STRING),
         'fecha_posible_saturacion_p90', CAST(a.fecha_posible_saturacion_p90 AS STRING),
         'fecha_posible_desabasto_p10', CAST(a.fecha_posible_desabasto_p10 AS STRING),
         'capacidad_operativa_bbl', round(s.capacidad_operativa_bbl, 0),
         'capacidad_disponible_actual_bbl', round(s.capacidad_disponible_bbl, 0),
         'capacidad_disponible_min_pronostico_bbl', round(a.capacidad_disponible_min_bbl, 0),
         'despacho_prom_7d_bbl', round(s.despacho_prom_7d_bbl, 0),
         'dias_autonomia_7d', round(s.dias_autonomia_7d, 1),
         'tanques_operativos', s.tanques_operativos, 'tanques_en_mantenimiento', s.tanques_en_mantenimiento,
         'pronostico_pct_llenado', p.pronostico,
         'alternativas_misma_region_producto', transform(coalesce(alt.alternativas, array()), x -> named_struct(
           'id_instalacion', x.id_instalacion, 'nombre_instalacion', x.nombre_instalacion,
           'pct_llenado', x.pct_llenado, 'capacidad_disponible_bbl', x.capacidad_disponible_bbl))
       )) AS contexto_json,
       coalesce(alt.alternativas, array()) AS alternativas
FROM alertas a
JOIN snap s USING (id_instalacion, id_producto)
LEFT JOIN pron p USING (serie_id)
LEFT JOIN alternativas alt USING (serie_id)
""")
contexto.createOrReplaceTempView("ctx_alertas")
print("alertas con contexto:", contexto.count())
display(contexto.select("serie_id", "nivel_alerta", F.size("alternativas").alias("n_alternativas"), "contexto_json"))

# COMMAND ----------

# DBTITLE 1,Generar recomendaciones con el LLM
PROMPT = """Eres planeador senior de logística de almacenamiento de combustibles de PEMEX. Con el CONTEXTO (JSON) de una alerta, redacta en español UNA acción logística concreta para los próximos días.
Reglas:
- Usa solo cifras y fechas del CONTEXTO; no inventes instalaciones, tarifas ni costos.
- Saturación: >90% de llenado. Desabasto: <15%. p50 es el escenario central y p90 el alto.
- Acciones válidas: reprogramar o diferir un arribo, adelantar despachos, transferir producto a una alternativa, priorizar liberar tanques en mantenimiento, solicitar reposición (desabasto) o solo monitorear.
- instalacion_alternativa debe ser un id_instalacion de alternativas_misma_region_producto, o NINGUNA.
- volumen_sugerido_bbl no puede exceder la capacidad_disponible_bbl de la alternativa elegida; usa 0 si no hay transferencia.
- fecha_limite_accion (YYYY-MM-DD) debe ser anterior a la primera fecha de riesgo.
- prioridad: ALTA si el p50 supera el umbral; MEDIA si solo el p90; BAJA en otro caso.
Formato de salida (obligatorio):
- Devuelve SIEMPRE los 7 campos: prioridad, accion_recomendada, justificacion, instalacion_alternativa, volumen_sugerido_bbl (número, 0 si no hay transferencia), fecha_limite_accion y riesgo_si_no_se_actua.
- accion_recomendada: máximo 60 palabras. justificacion: máximo 70 palabras. riesgo_si_no_se_actua: máximo 30 palabras.
CONTEXTO:
"""

# ai_query exige un solo campo de primer nivel en responseFormat (`plan`), pero devuelve
# los 7 campos planos en el JSON: se pide con FORMATO y se lee con PLAN.
PLAN = ("STRUCT<prioridad:STRING, accion_recomendada:STRING, justificacion:STRING, "
        "instalacion_alternativa:STRING, volumen_sugerido_bbl:DOUBLE, fecha_limite_accion:STRING, "
        "riesgo_si_no_se_actua:STRING>")
FORMATO = f"STRUCT<plan:{PLAN}>"

# Una sola llamada por alerta; el resultado se materializa antes de validar (las salidas del LLM no son deterministas).
generado = spark.sql(f"""
SELECT *, ai_query('{LLM}', concat(:prompt, contexto_json),
                   responseFormat => '{FORMATO}',
                   modelParameters => named_struct('temperature', 0.0, 'max_tokens', 1500),
                   failOnError => false) AS llm
FROM ctx_alertas
""", args={"prompt": PROMPT}).localCheckpoint()

# Con failOnError => false, ai_query devuelve STRUCT<result, errorMessage>.
tipo_result = generado.schema["llm"].dataType["result"].dataType.simpleString()
print("tipo de llm.result:", tipo_result)
r_col = F.from_json(F.col("llm.result"), PLAN) if tipo_result == "string" else F.col("llm.result")

resultado = (
    generado
    .withColumn("r", r_col)
    .select(
        "fecha_corte", "serie_id", "id_instalacion", "nombre_instalacion", "id_producto", "nombre_producto",
        "region", "nivel_alerta", "r.*", "contexto_json", "alternativas",
        F.col("llm.errorMessage").alias("error_llm"),
    )
    # La prioridad es una regla fija: se calcula en código, no se deja al LLM.
    .withColumn("prioridad", F.when(F.col("nivel_alerta") == "SATURACION", "ALTA")
                              .when(F.col("nivel_alerta") == "RIESGO_SATURACION", "MEDIA")
                              .otherwise("BAJA"))
    # Validaciones de código sobre la salida del LLM
    .withColumn("alt", F.filter("alternativas", lambda x: x["id_instalacion"] == F.col("instalacion_alternativa")))
    .withColumn("alternativa_valida", (F.col("instalacion_alternativa") == "NINGUNA") | (F.size("alt") == 1))
    .withColumn("volumen_valido", F.when(F.col("instalacion_alternativa") == "NINGUNA", F.coalesce(F.col("volumen_sugerido_bbl"), F.lit(0)) == 0)
                                   .otherwise(F.col("volumen_sugerido_bbl") <= F.try_element_at("alt", F.lit(1))["capacidad_disponible_bbl"]))
    .withColumn("validacion_ok", F.col("error_llm").isNull() & F.coalesce(F.col("alternativa_valida") & F.col("volumen_valido"), F.lit(False)))
    .drop("alt", "alternativas")
    .withColumn("modelo_llm", F.lit(LLM))
    .withColumn("ts_generado", F.current_timestamp())
)
display(resultado.select("serie_id", "prioridad", "accion_recomendada", "instalacion_alternativa", "volumen_sugerido_bbl", "fecha_limite_accion", "validacion_ok", "error_llm"))

# COMMAND ----------

# DBTITLE 1,Diagnóstico temporal: respuesta cruda
for fila in generado.select("serie_id", "llm.result", "llm.errorMessage").limit(2).collect():
    print(fila["serie_id"], "| error:", fila["errorMessage"])
    print(fila["result"][:1500])
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Guardar (idempotente por fecha de corte)
n_total = resultado.count()
n_error = resultado.filter("error_llm IS NOT NULL").count()
if n_total > 0 and n_error == n_total:
    raise RuntimeError(f"Todas las llamadas al LLM fallaron ({n_error}); no se escribe la tabla.")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DESTINO} (
  fecha_corte DATE COMMENT 'Último día con datos reales usado para la alerta.',
  serie_id STRING COMMENT 'id_instalacion|id_producto.',
  id_instalacion STRING, nombre_instalacion STRING, id_producto STRING, nombre_producto STRING, region STRING,
  nivel_alerta STRING COMMENT 'SATURACION, RIESGO_SATURACION o NORMAL (con posible desabasto).',
  prioridad STRING COMMENT 'ALTA (SATURACION), MEDIA (RIESGO_SATURACION) o BAJA; regla fija calculada en código.',
  accion_recomendada STRING COMMENT 'Acción logística recomendada en lenguaje natural.',
  justificacion STRING, instalacion_alternativa STRING COMMENT 'id_instalacion destino o NINGUNA.',
  volumen_sugerido_bbl DOUBLE COMMENT 'Volumen a transferir o diferir (bbl).',
  fecha_limite_accion STRING COMMENT 'Fecha límite sugerida (YYYY-MM-DD).',
  riesgo_si_no_se_actua STRING,
  contexto_json STRING COMMENT 'Contexto exacto enviado al LLM (trazabilidad).',
  error_llm STRING, alternativa_valida BOOLEAN, volumen_valido BOOLEAN,
  validacion_ok BOOLEAN COMMENT 'TRUE si la salida pasó las validaciones de código.',
  modelo_llm STRING, ts_generado TIMESTAMP
) COMMENT 'Plan de acción logística por alerta, redactado por IA generativa y validado en código.'
""")
resultado.write.mode("overwrite").option("replaceWhere", f"fecha_corte = DATE'{corte}'").saveAsTable(DESTINO)
print(f"OK: {n_total} recomendaciones ({n_error} con error del LLM) para {corte} en {DESTINO}")

# COMMAND ----------

# DBTITLE 1,Validación
# MAGIC %sql
# MAGIC SELECT fecha_corte, count(*) AS filas, count(DISTINCT serie_id) AS series,
# MAGIC        count_if(error_llm IS NOT NULL) AS errores_llm, count_if(NOT validacion_ok) AS fallan_validacion,
# MAGIC        count_if(accion_recomendada IS NULL OR trim(accion_recomendada) = '') AS sin_accion,
# MAGIC        count_if(prioridad = 'ALTA') AS alta, count_if(prioridad = 'MEDIA') AS media, count_if(prioridad = 'BAJA') AS baja,
# MAGIC        count_if(prioridad NOT IN ('ALTA', 'MEDIA', 'BAJA') OR prioridad IS NULL) AS prioridad_invalida
# MAGIC FROM <CATALOG>.pemex_alm_ml.plan_accion_alertas
# MAGIC GROUP BY fecha_corte ORDER BY fecha_corte DESC

# COMMAND ----------

