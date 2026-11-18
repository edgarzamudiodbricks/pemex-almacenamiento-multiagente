"""Cliente de la Genie Conversation API: texto, SQL generado y tabla de resultados."""

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import MessageStatus

import config

log = logging.getLogger(__name__)
TIMEOUT = timedelta(minutes=5)


@dataclass
class RespuestaGenie:
    conversation_id: str
    message_id: str
    texto: str
    sql: Optional[str] = None
    descripcion_sql: Optional[str] = None
    tabla: Optional[pd.DataFrame] = None
    sugerencias: list[str] = field(default_factory=list)
    error: Optional[str] = None


def _a_dataframe(statement_response) -> Optional[pd.DataFrame]:
    if not statement_response or not statement_response.manifest or not statement_response.manifest.schema:
        return None
    columnas = [c.name for c in statement_response.manifest.schema.columns]
    filas = (statement_response.result.data_array if statement_response.result else None) or []
    df = pd.DataFrame(filas, columns=columnas)
    for c in df.columns:
        convertida = pd.to_numeric(df[c], errors="coerce")
        if convertida.notna().sum() == df[c].notna().sum():  # solo convierte si todos los valores son numericos
            df[c] = convertida
    return df


def preguntar(w: WorkspaceClient, pregunta: str, conversation_id: Optional[str] = None,
              space_id: str = config.GENIE_SPACE_ID) -> RespuestaGenie:
    """Envia la pregunta a Genie reutilizando la conversacion si existe."""
    msg = None
    if conversation_id:
        try:
            msg = w.genie.create_message_and_wait(space_id, conversation_id, pregunta, timeout=TIMEOUT)
        except Exception as e:  # conversacion expirada o borrada: se abre una nueva
            log.warning("No se pudo continuar la conversacion Genie %s: %s", conversation_id, e)
    if msg is None:
        msg = w.genie.start_conversation_and_wait(space_id, pregunta, timeout=TIMEOUT)

    resp = RespuestaGenie(conversation_id=msg.conversation_id, message_id=msg.id, texto="")
    if msg.status != MessageStatus.COMPLETED:
        resp.error = (msg.error.error if msg.error else None) or f"Genie terminó con estado {msg.status}"
        resp.texto = f"Genie no pudo responder: {resp.error}"
        return resp

    textos = []
    for att in msg.attachments or []:
        if att.text and att.text.content:
            textos.append(att.text.content)
        if att.query and att.query.query and resp.sql is None:
            resp.sql = att.query.query
            resp.descripcion_sql = att.query.description
            try:
                r = w.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.id, att.attachment_id)
                resp.tabla = _a_dataframe(r.statement_response)
            except Exception as e:
                log.exception("Error leyendo resultado de Genie")
                resp.error = f"No se pudo leer el resultado de la consulta: {e}"
        if att.suggested_questions and att.suggested_questions.questions:
            resp.sugerencias = list(att.suggested_questions.questions)
    resp.texto = "\n\n".join(textos) or resp.descripcion_sql or "Genie generó la consulta mostrada abajo."
    return resp
