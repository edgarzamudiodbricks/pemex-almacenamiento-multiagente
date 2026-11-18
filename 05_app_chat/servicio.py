"""Logica de negocio independiente de la UI: rutea, ejecuta y persiste una respuesta."""

import logging
import time

import pandas as pd
from databricks.sdk import WorkspaceClient

import config
import escenario
import genie_client
from router import rutear
from storage import ChatStore

log = logging.getLogger(__name__)


def procesar(w: WorkspaceClient, store: ChatStore, cat: pd.DataFrame, conv: dict, pregunta: str, modo: str = "auto") -> dict:
    """Procesa la pregunta, guarda la respuesta del asistente en Lakebase y la devuelve."""
    inicio = time.time()
    previo = store.ultimo_escenario(conv["conversacion_id"])
    ruta = modo if modo in ("genie", "modelo") else rutear(pregunta, cat, previo)
    m = {"rol": "assistant", "ruta": ruta, "contenido": "", "resultado": None, "sql_generado": None,
         "parametros_escenario": None, "genie_conversation_id": None, "genie_message_id": None, "error": None}
    try:
        if ruta == "genie":
            r = genie_client.preguntar(w, pregunta, conv.get("genie_conversation_id"))
            if r.conversation_id != conv.get("genie_conversation_id"):
                store.fijar_genie_conversation(conv["conversacion_id"], r.conversation_id)
                conv["genie_conversation_id"] = r.conversation_id
            resultado = {"sugerencias": r.sugerencias}
            if r.tabla is not None:
                tabla = r.tabla.head(config.MAX_FILAS_GUARDADAS)
                resultado.update({"columnas": list(tabla.columns),
                                  "filas": tabla.astype(object).where(tabla.notna(), None).values.tolist(),
                                  "total_filas": len(r.tabla)})
            m.update(contenido=r.texto, sql_generado=r.sql, resultado=resultado, error=r.error,
                     genie_conversation_id=r.conversation_id, genie_message_id=r.message_id)
        else:
            r = escenario.simular(w, pregunta, cat, previo)
            m.update(contenido=r.texto, parametros_escenario=r.parametros,
                     resultado={"resumen": r.resumen, "detalle": r.detalle} if r.detalle else None)
    except Exception as e:
        log.exception("Error procesando pregunta (ruta=%s)", ruta)
        m.update(contenido="No pude completar la solicitud.", error=f"{type(e).__name__}: {e}")
    m["latencia_ms"] = int((time.time() - inicio) * 1000)
    store.guardar_mensaje(conv["conversacion_id"], rol="assistant", **{k: v for k, v in m.items() if k != "rol"})
    log.info("respuesta ruta=%s latencia_ms=%s error=%s", ruta, m["latencia_ms"], m["error"])
    return m
