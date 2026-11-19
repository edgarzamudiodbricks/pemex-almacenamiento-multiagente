"""Persistencia de conversaciones y mensajes en Lakebase Autoscaling (Postgres).

El esquema y las tablas se crean al arrancar con la identidad del service principal de la app,
de modo que el SP queda como owner. Cada conexion nueva del pool genera un token OAuth fresco.
"""

import json
import logging
from typing import Any, Optional

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

import config

log = logging.getLogger(__name__)


def resolver_endpoint(w: WorkspaceClient, valor: Optional[str]) -> str:
    """Devuelve la ruta projects/.../endpoints/... aunque el recurso inyecte otra ruta del branch."""
    if valor and "/endpoints/" in valor:
        return valor
    branch = config.LAKEBASE_BRANCH
    if valor and "/branches/" in valor:
        partes = valor.split("/")
        branch = "/".join(partes[: partes.index("branches") + 2])
    for ep in w.postgres.list_endpoints(parent=branch):
        if ep.spec and "READ_WRITE" in str(ep.spec.endpoint_type):
            return ep.name
    return config.LAKEBASE_ENDPOINT_DEFAULT


class ChatStore:
    def __init__(self, w: WorkspaceClient):
        self._w = w
        self._endpoint = resolver_endpoint(w, config.LAKEBASE_ENDPOINT)
        usuario_pg = config.PGUSER or w.current_user.me().user_name
        self.schema = config.CHAT_SCHEMA
        endpoint, cliente = self._endpoint, w

        class _ConexionConToken(psycopg.Connection):
            @classmethod
            def connect(cls, conninfo: str = "", **kwargs):
                kwargs["password"] = cliente.postgres.generate_database_credential(endpoint=endpoint).token
                return super().connect(conninfo, **kwargs)

        self._pool = ConnectionPool(
            conninfo=make_conninfo(
                host=config.PGHOST, port=config.PGPORT, dbname=config.PGDATABASE,
                user=usuario_pg, sslmode="require", connect_timeout=30,
                application_name="pemex-almacenamiento-chat",
            ),
            connection_class=_ConexionConToken,
            kwargs={"row_factory": dict_row, "autocommit": False},
            min_size=1, max_size=5, max_lifetime=2700, max_idle=600,
            check=ConnectionPool.check_connection, open=True,
        )
        log.info("Lakebase endpoint=%s usuario=%s", self._endpoint, usuario_pg)
        self._crear_esquema()

    def _crear_esquema(self) -> None:
        s = sql.Identifier(self.schema)
        ddl = [
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(s),
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}.conversaciones (
                conversacion_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                usuario TEXT NOT NULL,
                titulo TEXT NOT NULL,
                genie_conversation_id TEXT,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
                actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now())""").format(s),
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}.mensajes (
                mensaje_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                conversacion_id UUID NOT NULL REFERENCES {}.conversaciones (conversacion_id) ON DELETE CASCADE,
                rol TEXT NOT NULL CHECK (rol IN ('user', 'assistant')),
                contenido TEXT NOT NULL,
                ruta TEXT CHECK (ruta IN ('genie', 'modelo', 'sistema')),
                genie_conversation_id TEXT,
                genie_message_id TEXT,
                sql_generado TEXT,
                parametros_escenario JSONB,
                resultado JSONB,
                latencia_ms INTEGER,
                error TEXT,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now())""").format(s, s),
            sql.SQL("CREATE INDEX IF NOT EXISTS ix_mensajes_conversacion ON {}.mensajes (conversacion_id, mensaje_id)").format(s),
            sql.SQL("CREATE INDEX IF NOT EXISTS ix_conversaciones_usuario ON {}.conversaciones (usuario, actualizado_en DESC)").format(s),
            # Decisiones sobre el plan de acción (historial completo; la última por serie es la vigente)
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}.decisiones (
                decision_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                fecha_corte DATE NOT NULL,
                serie_id TEXT NOT NULL,
                decision TEXT NOT NULL CHECK (decision IN ('APROBADA', 'RECHAZADA', 'MODIFICADA')),
                accion_recomendada TEXT NOT NULL,
                accion_final TEXT,
                instalacion_alternativa TEXT,
                volumen_sugerido_bbl DOUBLE PRECISION,
                comentario TEXT,
                usuario TEXT NOT NULL,
                creado_en TIMESTAMPTZ NOT NULL DEFAULT now())""").format(s),
            sql.SQL("CREATE INDEX IF NOT EXISTS ix_decisiones_serie ON {}.decisiones (fecha_corte, serie_id, decision_id DESC)").format(s),
        ]
        with self._pool.connection() as conn:
            # lock para que varias instancias de la app no compitan al crear el esquema
            conn.execute("SELECT pg_advisory_xact_lock(hashtext('pemex_chat_ddl'))")
            for stmt in ddl:
                conn.execute(stmt)

    def _t(self, tabla: str) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(tabla))

    def crear_conversacion(self, usuario: str, titulo: str) -> str:
        with self._pool.connection() as conn:
            fila = conn.execute(
                sql.SQL("INSERT INTO {} (usuario, titulo) VALUES (%s, %s) RETURNING conversacion_id").format(self._t("conversaciones")),
                (usuario, titulo[:120]),
            ).fetchone()
        return str(fila["conversacion_id"])

    def listar_conversaciones(self, usuario: str, limite: int = 30) -> list[dict]:
        with self._pool.connection() as conn:
            return conn.execute(
                sql.SQL("SELECT conversacion_id::text AS conversacion_id, titulo, actualizado_en FROM {} "
                        "WHERE usuario = %s ORDER BY actualizado_en DESC LIMIT %s").format(self._t("conversaciones")),
                (usuario, limite),
            ).fetchall()

    def obtener_conversacion(self, conversacion_id: str) -> Optional[dict]:
        with self._pool.connection() as conn:
            return conn.execute(
                sql.SQL("SELECT conversacion_id::text AS conversacion_id, usuario, titulo, genie_conversation_id FROM {} "
                        "WHERE conversacion_id = %s").format(self._t("conversaciones")),
                (conversacion_id,),
            ).fetchone()

    def fijar_genie_conversation(self, conversacion_id: str, genie_conversation_id: str) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                sql.SQL("UPDATE {} SET genie_conversation_id = %s WHERE conversacion_id = %s").format(self._t("conversaciones")),
                (genie_conversation_id, conversacion_id),
            )

    def guardar_mensaje(self, conversacion_id: str, rol: str, contenido: str, ruta: Optional[str] = None,
                        genie_conversation_id: Optional[str] = None, genie_message_id: Optional[str] = None,
                        sql_generado: Optional[str] = None, parametros_escenario: Optional[dict] = None,
                        resultado: Optional[Any] = None, latencia_ms: Optional[int] = None,
                        error: Optional[str] = None) -> int:
        def _j(v):
            return Jsonb(json.loads(json.dumps(v, default=str))) if v is not None else None

        with self._pool.connection() as conn:
            fila = conn.execute(
                sql.SQL("""INSERT INTO {} (conversacion_id, rol, contenido, ruta, genie_conversation_id, genie_message_id,
                           sql_generado, parametros_escenario, resultado, latencia_ms, error)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING mensaje_id""").format(self._t("mensajes")),
                (conversacion_id, rol, contenido, ruta, genie_conversation_id, genie_message_id, sql_generado,
                 _j(parametros_escenario), _j(resultado), latencia_ms, error),
            ).fetchone()
            conn.execute(
                sql.SQL("UPDATE {} SET actualizado_en = now() WHERE conversacion_id = %s").format(self._t("conversaciones")),
                (conversacion_id,),
            )
        return int(fila["mensaje_id"])

    def listar_mensajes(self, conversacion_id: str) -> list[dict]:
        with self._pool.connection() as conn:
            return conn.execute(
                sql.SQL("SELECT * FROM {} WHERE conversacion_id = %s ORDER BY mensaje_id").format(self._t("mensajes")),
                (conversacion_id,),
            ).fetchall()

    def guardar_decision(self, fecha_corte: str, serie_id: str, decision: str, accion_recomendada: str,
                         accion_final: Optional[str], instalacion_alternativa: Optional[str],
                         volumen_sugerido_bbl: Optional[float], comentario: Optional[str], usuario: str) -> int:
        with self._pool.connection() as conn:
            fila = conn.execute(
                sql.SQL("""INSERT INTO {} (fecha_corte, serie_id, decision, accion_recomendada, accion_final,
                           instalacion_alternativa, volumen_sugerido_bbl, comentario, usuario)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING decision_id""").format(self._t("decisiones")),
                (fecha_corte, serie_id, decision, accion_recomendada, accion_final, instalacion_alternativa,
                 volumen_sugerido_bbl, comentario, usuario),
            ).fetchone()
        return int(fila["decision_id"])

    def decisiones_vigentes(self, fecha_corte: str) -> dict[str, dict]:
        """Última decisión por serie para una fecha de corte."""
        with self._pool.connection() as conn:
            filas = conn.execute(
                sql.SQL("""SELECT DISTINCT ON (serie_id) serie_id, decision, accion_final, comentario, usuario, creado_en
                           FROM {} WHERE fecha_corte = %s ORDER BY serie_id, decision_id DESC""").format(self._t("decisiones")),
                (fecha_corte,),
            ).fetchall()
        return {f["serie_id"]: f for f in filas}

    def ultimo_escenario(self, conversacion_id: str) -> Optional[dict]:
        """Parametros del ultimo escenario de la conversacion (para preguntas de seguimiento)."""
        with self._pool.connection() as conn:
            fila = conn.execute(
                sql.SQL("SELECT parametros_escenario FROM {} WHERE conversacion_id = %s AND ruta = 'modelo' "
                        "AND rol = 'assistant' AND parametros_escenario ? 'serie_id' ORDER BY mensaje_id DESC LIMIT 1").format(self._t("mensajes")),
                (conversacion_id,),
            ).fetchone()
        return fila["parametros_escenario"] if fila else None
