"""Arranque: crea el esquema de Lakebase con la identidad del SP y, opcionalmente, ejecuta un smoke test.

RUN_SMOKE_TEST=true ejecuta una pregunta de datos (Genie) y un escenario what-if (modelo) como el SP,
los guarda en Lakebase con usuario 'smoke-test' y escribe el resultado en los logs. Con --smoke corre en segundo
plano (ver app.yaml) para no bloquear el arranque de Streamlit.
"""

import logging
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

import escenario
import plan_accion
import servicio
from storage import ChatStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stdout)
log = logging.getLogger("init_app")

PREGUNTAS_SMOKE = [
    "¿Cuáles son las 5 instalaciones con mayor porcentaje de llenado actual?",
    "¿Qué pasa si la capacidad operativa de la TAD Santa Catarina (Monterrey) para diésel baja a 80%?",
]


def main(smoke: bool) -> int:
    w = WorkspaceClient(config=Config(http_timeout_seconds=180))
    store = ChatStore(w)
    with store._pool.connection() as conn:
        fila = conn.execute("SELECT current_user AS usuario, nspowner::regrole::text AS owner FROM pg_namespace WHERE nspname = %s",
                            (store.schema,)).fetchone()
    log.info("INIT_OK esquema=%s owner=%s current_user=%s", store.schema, fila["owner"], fila["usuario"])

    if not smoke:
        return 0
    # Chequeo de solo lectura en cada arranque: el SP puede leer el plan de acción y la tabla de decisiones.
    try:
        df_plan = plan_accion.cargar_plan(w)
        corte = df_plan.fecha_corte.iloc[0] if not df_plan.empty else None
        n_dec = len(store.decisiones_vigentes(corte)) if corte else 0
        log.info("PLAN_OK filas=%s corte=%s decisiones_vigentes=%s", len(df_plan), corte, n_dec)
    except Exception:
        log.exception("PLAN_FAIL (revisar SELECT del SP sobre pemex_alm_ml.plan_accion_alertas)")
    if os.getenv("RUN_SMOKE_TEST", "false").lower() != "true":
        return 0
    try:
        cat = escenario.cargar_catalogo(w)
        log.info("SMOKE catalogo series=%s", len(cat))
    except Exception:
        log.exception("SMOKE_FAIL catalogo (revisar grants UC del SP)")
        cat = None
    cid = store.crear_conversacion("smoke-test", "Smoke test de arranque")
    conv = store.obtener_conversacion(cid)
    for pregunta in PREGUNTAS_SMOKE:
        store.guardar_mensaje(cid, "user", pregunta)
        if cat is None:
            break
        m = servicio.procesar(w, store, cat, conv, pregunta)
        estado = "SMOKE_OK" if not m["error"] else "SMOKE_FAIL"
        log.info("%s ruta=%s latencia_ms=%s sql=%s error=%s texto=%s", estado, m["ruta"], m["latencia_ms"],
                 bool(m["sql_generado"]), m["error"], m["contenido"][:160].replace("\n", " "))
    log.info("SMOKE_DONE conversacion_id=%s", cid)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(smoke="--smoke" in sys.argv))
    except Exception:
        log.exception("INIT_FAIL")
        sys.exit(0)  # la UI sigue arrancando y muestra el error de conexion
