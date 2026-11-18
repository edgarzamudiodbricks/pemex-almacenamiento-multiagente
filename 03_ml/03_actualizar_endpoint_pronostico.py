# Databricks notebook source
# DBTITLE 1,Descripción
# MAGIC %md
# MAGIC # Actualizar endpoint de pronóstico de capacidad
# MAGIC
# MAGIC Tarea 4 del Job. Resuelve el alias `champion` a una **versión exacta** y apunta el endpoint `modelo_pronostico_capacidad` a esa versión (CPU / Small / scale-to-zero). Si ya la sirve, no hace nada (idempotente). Solo actualiza un endpoint que sirve exclusivamente este modelo. Al final hace un smoke test contra la tabla `pronostico_capacidad_diario` generada con esa misma versión.

# COMMAND ----------

# DBTITLE 1,Resolver versión y actualizar endpoint
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointStateConfigUpdate,
    EndpointStateReady,
    ServedEntityInput,
    ServingModelWorkloadType,
)
from mlflow import MlflowClient

CATALOG = "<CATALOG>"  # parámetro: ver README
MODEL_NAME = f"{CATALOG}.pemex_alm_ml.modelo_pronostico_capacidad"
ENDPOINT = "modelo_pronostico_capacidad"
w = WorkspaceClient()

version = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(MODEL_NAME, "champion").version
ep = w.serving_endpoints.get(ENDPOINT)
entidades = list(ep.config.served_entities or []) if ep.config else []
if len(entidades) != 1 or entidades[0].entity_name != MODEL_NAME:
    raise ValueError(f"{ENDPOINT} sirve {[e.entity_name for e in entidades]}; no se reemplaza automáticamente.")
actual = entidades[0].entity_version
print(f"champion = v{version} | endpoint sirve v{actual}")


def esperar_ready(nombre, timeout_s=1800, poll_s=20):
    fin, fallas = time.time() + timeout_s, {EndpointStateConfigUpdate.UPDATE_FAILED, EndpointStateConfigUpdate.UPDATE_CANCELED}
    while time.time() < fin:
        st = w.serving_endpoints.get(nombre).state
        if st.ready == EndpointStateReady.READY and st.config_update == EndpointStateConfigUpdate.NOT_UPDATING:
            return st
        if st.config_update in fallas:
            raise RuntimeError(f"{nombre}: actualización fallida ({st.config_update.value})")
        time.sleep(poll_s)
    raise TimeoutError(f"{nombre} no quedó listo en {timeout_s}s")


if str(actual) == str(version):
    print("Sin cambios: el endpoint ya sirve la versión champion.")
else:
    w.serving_endpoints.update_config(
        name=ENDPOINT,
        served_entities=[ServedEntityInput(
            entity_name=MODEL_NAME, entity_version=str(version),
            workload_type=ServingModelWorkloadType("CPU"), workload_size="Small", scale_to_zero_enabled=True,
        )],
    )
    print("Actualización enviada; esperando READY...")
    print(esperar_ready(ENDPOINT))

servida = w.serving_endpoints.get(ENDPOINT).config.served_entities[0].entity_version
assert str(servida) == str(version), f"el endpoint sirve v{servida}, se esperaba v{version}"
print(f"OK: {ENDPOINT} sirve v{servida}")

# COMMAND ----------

# DBTITLE 1,Smoke test contra la tabla de pronóstico
ref = spark.sql(f"""
  SELECT serie_id, CAST(fecha AS STRING) AS fecha, frac_capacidad_operativa, pct_llenado_p50
  FROM {CATALOG}.pemex_alm_ml.pronostico_capacidad_diario
  WHERE version_modelo = '{version}' AND horizonte_dias IN (1, 15, 30)
  ORDER BY rand(7) LIMIT 5""").toPandas()
assert len(ref) > 0, f"no hay pronósticos de la v{version} en la tabla para comparar"
registros = ref[["serie_id", "fecha", "frac_capacidad_operativa"]].to_dict("records")
pred = w.serving_endpoints.query(name=ENDPOINT, dataframe_records=registros).predictions
ref["endpoint_p50"] = [p["pct_llenado_p50"] for p in pred]
ref["dif"] = (ref["endpoint_p50"] - ref["pct_llenado_p50"]).abs()
display(ref)
assert (ref["dif"] <= 0.01).all(), "el endpoint no coincide con la tabla de pronóstico"
print("Smoke test OK")

# COMMAND ----------

