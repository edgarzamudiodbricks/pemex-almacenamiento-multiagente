"""PEMEX Almacenamiento - Asistente de inventarios y escenarios (Databricks App, Streamlit)."""

import logging
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

import escenario
import servicio
from storage import ChatStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pemex-chat")

st.set_page_config(page_title="PEMEX Almacenamiento – Asistente", page_icon="🛢️", layout="wide")

EJEMPLOS = [
    "¿Cuáles son las 5 instalaciones con mayor porcentaje de llenado actual?",
    "¿Qué series tienen riesgo de saturación en los próximos 30 días?",
    "¿Qué pasa si la capacidad operativa de la TAD Santa Catarina (Monterrey) para diésel baja a 80%?",
    "Pronóstico de llenado de combustóleo en la Terminal Marítima Guaymas",
]
ETIQUETA_RUTA = {"genie": "Genie Agent", "modelo": "Modelo de pronóstico", "sistema": "Sistema"}


# ---------------------------------------------------------------- recursos compartidos
@st.cache_resource
def cliente() -> WorkspaceClient:
    return WorkspaceClient(config=Config(http_timeout_seconds=180))


@st.cache_resource
def almacen() -> ChatStore:
    return ChatStore(cliente())


@st.cache_data(ttl=3600, show_spinner="Cargando catálogo de series…")
def catalogo() -> pd.DataFrame:
    return escenario.cargar_catalogo(cliente())


def usuario_actual() -> str:
    h = st.context.headers
    return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "local"


# ---------------------------------------------------------------- render
def grafico_escenario(detalle: pd.DataFrame) -> alt.Chart:
    filas = []
    for nombre, pref in (("Base", "base_"), ("Escenario", "esc_")):
        if f"{pref}pct_llenado_p50" in detalle:
            filas.append(pd.DataFrame({"fecha": pd.to_datetime(detalle.fecha), "serie": nombre,
                                       "p10": detalle[f"{pref}pct_llenado_p10"], "p50": detalle[f"{pref}pct_llenado_p50"],
                                       "p90": detalle[f"{pref}pct_llenado_p90"]}))
    df = pd.concat(filas)
    color = alt.Color("serie:N", title=None, scale=alt.Scale(domain=["Base", "Escenario"], range=["#1f77b4", "#d62728"]))
    banda = alt.Chart(df).mark_area(opacity=0.15).encode(x=alt.X("fecha:T", title="Fecha"), y=alt.Y("p10:Q", title="% llenado"), y2="p90:Q", color=color)
    linea = alt.Chart(df).mark_line(point=True).encode(x="fecha:T", y=alt.Y("p50:Q", scale=alt.Scale(zero=False)), color=color,
                                                       tooltip=["serie", alt.Tooltip("fecha:T"), "p10", "p50", "p90"])
    return (banda + linea).properties(height=320)


def render_mensaje(m: dict, clave: str) -> None:
    with st.chat_message(m["rol"]):
        if m["rol"] == "assistant" and m.get("ruta"):
            lat = f" · {m['latencia_ms'] / 1000:.1f}s" if m.get("latencia_ms") else ""
            st.caption(f"Ruta: **{ETIQUETA_RUTA.get(m['ruta'], m['ruta'])}**{lat}")
        st.markdown(m["contenido"])
        if m.get("error"):
            st.error(m["error"])
        res = m.get("resultado") or {}
        if m.get("ruta") == "genie":
            if m.get("sql_generado"):
                with st.expander("SQL generado por Genie"):
                    st.code(m["sql_generado"], language="sql")
            if res.get("filas"):
                st.dataframe(pd.DataFrame(res["filas"], columns=res["columnas"]), width="stretch", hide_index=True)
                if res.get("total_filas", 0) > len(res["filas"]):
                    st.caption(f"Se muestran {len(res['filas'])} de {res['total_filas']} filas.")
            if res.get("sugerencias"):
                st.caption("Sugerencias: " + " · ".join(res["sugerencias"]))
        elif m.get("ruta") == "modelo" and res.get("detalle"):
            st.dataframe(pd.DataFrame(res["resumen"]), width="stretch", hide_index=True)
            detalle = pd.DataFrame(res["detalle"])
            st.altair_chart(grafico_escenario(detalle), width="stretch")
            with st.expander("Detalle diario"):
                st.dataframe(detalle, width="stretch", hide_index=True)


# ---------------------------------------------------------------- UI
def main() -> None:
    try:
        store = almacen()
    except Exception as e:
        log.exception("No se pudo inicializar Lakebase")
        st.error(f"No se pudo conectar a Lakebase: {e}")
        st.stop()
    usuario = usuario_actual()
    st.session_state.setdefault("conversacion_id", None)

    with st.sidebar:
        st.header("Conversaciones")
        if st.button("➕ Nueva conversación", width="stretch"):
            st.session_state.conversacion_id = None
            st.rerun()
        for c in store.listar_conversaciones(usuario):
            activo = c["conversacion_id"] == st.session_state.conversacion_id
            if st.button(("▶ " if activo else "") + c["titulo"][:48], key=f"conv_{c['conversacion_id']}", width="stretch"):
                st.session_state.conversacion_id = c["conversacion_id"]
                st.rerun()
        st.divider()
        modo = st.radio("Ruteo", ["auto", "genie", "modelo"], horizontal=True,
                        format_func={"auto": "Automático", "genie": "Genie", "modelo": "Modelo"}.get)
        st.caption(f"Usuario: {usuario}")

    st.title("PEMEX Almacenamiento – Asistente")
    st.caption("Preguntas de datos → Genie Agent · Pronósticos y escenarios what-if → modelo `modelo_pronostico_capacidad`")

    conv: Optional[dict] = store.obtener_conversacion(st.session_state.conversacion_id) if st.session_state.conversacion_id else None
    if conv and conv["usuario"] != usuario:
        conv, st.session_state.conversacion_id = None, None

    if conv:
        for m in store.listar_mensajes(conv["conversacion_id"]):
            render_mensaje(m, str(m["mensaje_id"]))
    else:
        st.info("Ejemplos:\n\n" + "\n".join(f"* {e}" for e in EJEMPLOS))

    pregunta = st.chat_input("Escribe tu pregunta…")
    if not pregunta:
        return
    if not conv:
        cid = store.crear_conversacion(usuario, pregunta)
        st.session_state.conversacion_id = cid
        conv = store.obtener_conversacion(cid)
    store.guardar_mensaje(conv["conversacion_id"], "user", pregunta)
    render_mensaje({"rol": "user", "contenido": pregunta}, "nuevo_user")
    with st.spinner("Procesando…"):
        try:
            cat = catalogo()
        except Exception as e:
            log.exception("No se pudo cargar el catálogo de series")
            st.error(f"No se pudo leer el catálogo de series (permisos de Unity Catalog del SP): {e}")
            st.stop()
        respuesta = servicio.procesar(cliente(), store, cat, conv, pregunta, modo)
    render_mensaje(respuesta, "nuevo_assistant")


main()
