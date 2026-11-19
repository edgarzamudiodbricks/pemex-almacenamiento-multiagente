"""Lectura del plan de acción generado por el Job (tabla pemex_alm_ml.plan_accion_alertas)."""

import pandas as pd
from databricks.sdk import WorkspaceClient

import config
from escenario import ejecutar_sql

ORDEN_PRIORIDAD = {"ALTA": 0, "MEDIA": 1, "BAJA": 2}


def cargar_plan(w: WorkspaceClient) -> pd.DataFrame:
    """Recomendaciones de la última fecha de corte, ordenadas por prioridad y fecha límite."""
    df = ejecutar_sql(w, f"""
        SELECT CAST(fecha_corte AS STRING) AS fecha_corte, serie_id, id_instalacion, nombre_instalacion,
               nombre_producto, region, nivel_alerta, prioridad, accion_recomendada, justificacion,
               instalacion_alternativa, volumen_sugerido_bbl, fecha_limite_accion, riesgo_si_no_se_actua,
               validacion_ok, modelo_llm, CAST(ts_generado AS STRING) AS ts_generado
        FROM {config.TABLA_PLAN}
        WHERE fecha_corte = (SELECT max(fecha_corte) FROM {config.TABLA_PLAN})""")
    if df.empty:
        return df
    df["volumen_sugerido_bbl"] = pd.to_numeric(df["volumen_sugerido_bbl"], errors="coerce").fillna(0)
    df["validacion_ok"] = df["validacion_ok"].astype(str).str.lower() == "true"
    df["_orden"] = df["prioridad"].map(ORDEN_PRIORIDAD).fillna(9)
    return df.sort_values(["_orden", "fecha_limite_accion", "serie_id"]).drop(columns="_orden").reset_index(drop=True)
