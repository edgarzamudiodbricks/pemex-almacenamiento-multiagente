"""Escenarios what-if: resuelve serie, lee pronostico base y llama al endpoint de Model Serving."""

import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition, ExecuteStatementRequestOnWaitTimeout, Format, StatementParameterListItem, StatementState,
)

import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------- SQL warehouse
def ejecutar_sql(w: WorkspaceClient, consulta: str, parametros: Optional[dict] = None, timeout_s: int = 300) -> pd.DataFrame:
    params = [StatementParameterListItem(name=k, value=str(v), type="INT" if isinstance(v, int) else "STRING")
              for k, v in (parametros or {}).items()]
    r = w.statement_execution.execute_statement(
        statement=consulta, warehouse_id=config.WAREHOUSE_ID, parameters=params or None,
        wait_timeout="30s", on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE, format=Format.JSON_ARRAY,
    )
    fin = time.time() + timeout_s
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > fin:
            w.statement_execution.cancel_execution(r.statement_id)
            raise TimeoutError("La consulta al SQL warehouse excedió el tiempo límite")
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Consulta SQL fallida: {r.status.error.message if r.status.error else r.status.state}")
    columnas = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame((r.result.data_array if r.result else None) or [], columns=columnas)


def cargar_catalogo(w: WorkspaceClient) -> pd.DataFrame:
    """Series validas (instalacion x producto) desde gold."""
    return ejecutar_sql(w, f"""
        SELECT DISTINCT serie_id, id_instalacion, nombre_instalacion, tipo_instalacion, region,
               id_producto, nombre_producto
        FROM {config.TABLA_SERIES}""")


# ---------------------------------------------------------------- resolucion de entidades
def normalizar(texto: str) -> str:
    t = unicodedata.normalize("NFKD", texto.lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", t)).strip()


PREFIJOS = ("refineria ", "terminal maritima ", "terminal flotante ", "tad ")
PALABRAS_PRODUCTO = {"olmeca", "istmo", "maya"}
TIPOS_TEXTO = {
    "REFINERIA": r"\b(refineria|ref)\b",
    "TAD": r"\b(tad|tar|terminal de almacenamiento)\b",
    "TERMINAL_MARITIMA": r"\b(terminal maritima|tm|tmr|puerto)\b",
    "TERMINAL_CRUDO": r"\b(terminal maritima|terminal flotante|tmc)\b",
}
PRODUCTOS = [
    ("DIE", r"\bdiesel\b"),
    ("GAS-PRE", r"\bpremium\b"),
    ("GAS-REG", r"\b(regular|magna)\b"),
    ("TURB", r"\b(turbosina|jet|turbo)\b"),
    ("COMB", r"\bcombustoleo\b"),
    ("GLP", r"\b(glp|gas lp|gas l p)\b"),
    ("CRU-MAYA", r"\bmaya\b"),
    ("CRU-ISTMO", r"\bistmo\b"),
    ("CRU-OLMECA", r"\bolmeca\b"),
]
GENERICOS = [(r"\bgasolinas?\b", ("GAS-PRE", "GAS-REG")), (r"\b(crudo|petroleo)\b", ("CRU-MAYA", "CRU-ISTMO", "CRU-OLMECA"))]


def _alias_instalacion(nombre: str) -> set[str]:
    n = normalizar(nombre)
    alias = {n}
    sin_par = normalizar(re.sub(r"\(.*?\)", " ", nombre))
    alias.add(sin_par)
    for p in re.findall(r"\((.*?)\)", nombre):
        alias.add(normalizar(p))
    for a in list(alias):
        for pref in PREFIJOS:
            if a.startswith(pref):
                alias.add(a[len(pref):])
        alias.add(re.sub(r"^(ing|gral)\s+", "", a))
    return {a for a in alias if len(a) >= 4 and a not in PALABRAS_PRODUCTO}


@dataclass
class Resolucion:
    serie_id: Optional[str] = None
    id_instalacion: Optional[str] = None
    nombre_instalacion: Optional[str] = None
    id_producto: Optional[str] = None
    nombre_producto: Optional[str] = None
    pregunta_aclaracion: Optional[str] = None


def resolver_serie(texto: str, catalogo: pd.DataFrame, previo: Optional[dict] = None) -> Resolucion:
    """Identifica instalacion y producto. `previo` permite preguntas de seguimiento sin repetirlos."""
    norm = f" {normalizar(texto)} "
    inst = catalogo.drop_duplicates("id_instalacion")[["id_instalacion", "nombre_instalacion", "tipo_instalacion"]]

    # 1) id exacto (TAD-MTY) o alias del nombre; gana el alias mas largo
    candidatos: dict[str, tuple[int, str]] = {}
    for r in inst.itertuples():
        id_norm = normalizar(r.id_instalacion)
        if re.search(rf"\b{re.escape(id_norm)}\b", norm):
            candidatos[r.id_instalacion] = (100, id_norm)
            continue
        for a in _alias_instalacion(r.nombre_instalacion):
            if re.search(rf"\b{re.escape(a)}\b", norm) and len(a) > candidatos.get(r.id_instalacion, (0, ""))[0]:
                candidatos[r.id_instalacion] = (len(a), a)
    if candidatos:
        mejor = max(v[0] for v in candidatos.values())
        candidatos = {k: v for k, v in candidatos.items() if v[0] == mejor}
    if len(candidatos) > 1:  # desempate por tipo mencionado (refineria / TAD / terminal)
        tipos = dict(zip(inst.id_instalacion, inst.tipo_instalacion))
        filtrados = {k: v for k, v in candidatos.items() if re.search(TIPOS_TEXTO.get(tipos[k], r"$^"), norm)}
        candidatos = filtrados or candidatos

    # 2) producto (sobre el texto sin el alias de la instalacion, p.ej. "Refineria Olmeca")
    texto_prod = norm
    for _, alias in candidatos.values():
        texto_prod = texto_prod.replace(f" {alias} ", " ")
    productos = [pid for pid, pat in PRODUCTOS if re.search(pat, texto_prod)]
    if not productos:
        for pat, opciones in GENERICOS:
            if re.search(pat, texto_prod):
                productos = list(opciones)

    if not candidatos and previo:
        candidatos = {previo["id_instalacion"]: (0, "")}
        productos = productos or [previo["id_producto"]]

    # 3) cruzar con las series validas
    series = catalogo[catalogo.id_instalacion.isin(candidatos.keys())] if candidatos else catalogo.iloc[0:0]
    if productos:
        series = series[series.id_producto.isin(productos)]

    res = Resolucion()
    if not candidatos:
        res.pregunta_aclaracion = ("No identifiqué la instalación. Indícala por nombre o clave "
                                   "(p. ej. `TAD-MTY`, `TAD Santa Catarina (Monterrey)`, `REF-SCR`).")
        return res
    if series.empty:
        disponibles = catalogo[catalogo.id_instalacion.isin(candidatos.keys())]
        res.pregunta_aclaracion = "No hay serie de pronóstico para esa combinación. Opciones disponibles:\n\n" + "\n".join(
            f"* {r.nombre_instalacion} – {r.nombre_producto} (`{r.serie_id}`)" for r in disponibles.itertuples())
        return res
    if len(series) > 1:
        res.pregunta_aclaracion = "Hay varias series posibles; indica cuál:\n\n" + "\n".join(
            f"* {r.nombre_instalacion} – {r.nombre_producto} (`{r.serie_id}`)" for r in series.head(15).itertuples())
        return res
    s = series.iloc[0]
    return Resolucion(s.serie_id, s.id_instalacion, s.nombre_instalacion, s.id_producto, s.nombre_producto)


# ---------------------------------------------------------------- parametros del escenario
NUM = r"(\d+(?:[.,]\d+)?)"
PCT = r"\s*(?:%|por ?ciento)"
VERBOS_BAJA = r"(?:baja|bajar|reduce|reducir|reduzca|disminuye|disminuir|cae|caer|pierde|perder|recorta|recortar)\w*"
VERBOS_SUBE = r"(?:sube|subir|aumenta|aumentar|incrementa|incrementar|crece|crecer)\w*"


def _f(x: str) -> float:
    return float(x.replace(",", "."))


def extraer_cambio_capacidad(texto: str) -> Optional[dict]:
    """Devuelve {'tipo': 'absoluto'|'relativo', 'valor': fraccion} o None si no hay cambio."""
    t = texto.lower()
    m = re.search(rf"{VERBOS_BAJA}\s+(?:en\s+)?(?:un\s+)?{NUM}{PCT}", t)
    if m:
        return {"tipo": "relativo", "valor": -_f(m.group(1)) / 100}
    m = re.search(rf"{VERBOS_SUBE}\s+(?:en\s+)?(?:un\s+)?{NUM}{PCT}", t)
    if m:
        return {"tipo": "relativo", "valor": _f(m.group(1)) / 100}
    m = re.search(rf"(?:\ba|\bal|\bdel|\bde|\ben|\bhasta)\s+(?:un\s+)?{NUM}{PCT}", t) or re.search(rf"{NUM}{PCT}", t)
    if m:
        return {"tipo": "absoluto", "valor": _f(m.group(1)) / 100}
    m = re.search(r"\b(0[.,]\d+|1[.,]0+)\b", t)
    if m:
        return {"tipo": "absoluto", "valor": _f(m.group(1))}
    return None


def extraer_horizonte(texto: str) -> int:
    m = re.search(r"(?:proxim|siguient)\w*\s+(\d+)\s+dias", normalizar(texto))
    return max(1, min(config.HORIZONTE_MAX_DIAS, int(m.group(1)))) if m else config.HORIZONTE_MAX_DIAS


# ---------------------------------------------------------------- datos base y endpoint
def pronostico_base(w: WorkspaceClient, serie_id: str, horizonte: int) -> pd.DataFrame:
    df = ejecutar_sql(w, f"""
        SELECT CAST(fecha AS STRING) AS fecha, horizonte_dias, frac_capacidad_operativa, pct_llenado_actual,
               capacidad_nominal_bbl, capacidad_operativa_bbl, CAST(fecha_corte AS STRING) AS fecha_corte, version_modelo
        FROM {config.TABLA_PRONOSTICO}
        WHERE serie_id = :serie_id AND horizonte_dias <= :horizonte
          AND fecha_corte = (SELECT max(fecha_corte) FROM {config.TABLA_PRONOSTICO} WHERE serie_id = :serie_id)
        ORDER BY fecha""", {"serie_id": serie_id, "horizonte": horizonte})
    for c in ("horizonte_dias", "frac_capacidad_operativa", "pct_llenado_actual", "capacidad_nominal_bbl", "capacidad_operativa_bbl"):
        df[c] = pd.to_numeric(df[c])
    return df


def alerta_actual(w: WorkspaceClient, serie_id: str) -> Optional[dict]:
    df = ejecutar_sql(w, f"""
        SELECT nivel_alerta, CAST(fecha_posible_saturacion_p90 AS STRING) AS fecha_posible_saturacion_p90,
               CAST(fecha_posible_desabasto_p10 AS STRING) AS fecha_posible_desabasto_p10
        FROM {config.TABLA_ALERTAS}
        WHERE serie_id = :serie_id ORDER BY fecha_corte DESC LIMIT 1""", {"serie_id": serie_id})
    return df.iloc[0].to_dict() if not df.empty else None


def predecir(w: WorkspaceClient, serie_id: str, fechas: list[str], fracs: list[float]) -> pd.DataFrame:
    """Una llamada por escenario: el modelo no admite (serie_id, fecha) repetidos en el mismo request."""
    registros = [{"serie_id": serie_id, "fecha": f, "frac_capacidad_operativa": float(x)} for f, x in zip(fechas, fracs)]
    ultimo_error = None
    for intento in range(4):  # el endpoint escala a cero: el primer request puede expirar
        try:
            pred = w.serving_endpoints.query(name=config.SERVING_ENDPOINT, dataframe_records=registros).predictions
            df = pd.DataFrame(pred)
            df["fecha"] = df["fecha"].astype(str).str[:10]
            return df.drop_duplicates("fecha")[["fecha", "pct_llenado_p10", "pct_llenado_p50", "pct_llenado_p90"]]
        except Exception as e:
            ultimo_error = e
            log.warning("Endpoint intento %s falló: %s", intento + 1, e)
            time.sleep(10 * (intento + 1))
    raise RuntimeError(f"El endpoint {config.SERVING_ENDPOINT} no respondió: {ultimo_error}")


@dataclass
class ResultadoEscenario:
    texto: str
    parametros: dict
    detalle: list[dict] = field(default_factory=list)
    resumen: list[dict] = field(default_factory=list)


def simular(w: WorkspaceClient, texto: str, catalogo: pd.DataFrame, previo: Optional[dict] = None) -> ResultadoEscenario:
    res = resolver_serie(texto, catalogo, previo)
    if res.pregunta_aclaracion:
        return ResultadoEscenario(texto=res.pregunta_aclaracion, parametros={"aclaracion": True})

    horizonte = extraer_horizonte(texto)
    cambio = extraer_cambio_capacidad(texto)
    base = pronostico_base(w, res.serie_id, horizonte)
    if base.empty:
        return ResultadoEscenario(texto=f"No hay pronóstico base para `{res.serie_id}`.", parametros={"serie_id": res.serie_id})

    frac_base = base["frac_capacidad_operativa"].tolist()
    notas = []
    if cambio is None:
        frac_esc = frac_base
    elif cambio["tipo"] == "absoluto":
        frac_esc = [cambio["valor"]] * len(base)
    else:
        frac_esc = [f * (1 + cambio["valor"]) for f in frac_base]
    acotada = [min(1.0, max(0.05, f)) for f in frac_esc]
    if acotada != frac_esc:
        notas.append("La capacidad operativa se acotó al rango 5%–100% de la capacidad nominal.")
    frac_esc = acotada

    fechas = base["fecha"].tolist()
    p_base = predecir(w, res.serie_id, fechas, frac_base).add_prefix("base_").rename(columns={"base_fecha": "fecha"})
    det = base[["fecha", "horizonte_dias", "frac_capacidad_operativa"]].merge(p_base, on="fecha")
    hay_escenario = cambio is not None
    if hay_escenario:
        p_esc = predecir(w, res.serie_id, fechas, frac_esc).add_prefix("esc_").rename(columns={"esc_fecha": "fecha"})
        det = det.merge(p_esc, on="fecha")
        det["frac_escenario"] = frac_esc[: len(det)]
        det["delta_p50_pp"] = det["esc_pct_llenado_p50"] - det["base_pct_llenado_p50"]

    final = det.iloc[-1]
    resumen = []
    for q in ("p10", "p50", "p90"):
        fila = {"percentil": q, "base_final": round(final[f"base_pct_llenado_{q}"], 2),
                "base_max": round(det[f"base_pct_llenado_{q}"].max(), 2)}
        if hay_escenario:
            fila.update({"escenario_final": round(final[f"esc_pct_llenado_{q}"], 2),
                         "escenario_max": round(det[f"esc_pct_llenado_{q}"].max(), 2),
                         "delta_final_pp": round(final[f"esc_pct_llenado_{q}"] - final[f"base_pct_llenado_{q}"], 2)})
        resumen.append(fila)

    alerta = alerta_actual(w, res.serie_id)
    fb, fe = sum(frac_base) / len(frac_base), sum(frac_esc) / len(frac_esc)
    lineas = [f"**{res.nombre_instalacion} – {res.nombre_producto}** (`{res.serie_id}`), horizonte {len(det)} días "
              f"({det.fecha.iloc[0]} a {det.fecha.iloc[-1]}), llenado actual {base.pct_llenado_actual.iloc[0]:.1f}%."]
    if hay_escenario:
        lineas.append(f"Capacidad operativa promedio: base {fb:.0%} → escenario {fe:.0%}.")
        lineas.append(f"Al final del horizonte, p50 {final.base_pct_llenado_p50:.1f}% → {final.esc_pct_llenado_p50:.1f}% "
                      f"({final.delta_p50_pp:+.2f} pp); banda p10–p90 del escenario "
                      f"{final.esc_pct_llenado_p10:.1f}%–{final.esc_pct_llenado_p90:.1f}%.")
    else:
        lineas.append(f"Sin cambio de capacidad indicado: se muestra el pronóstico base (capacidad operativa {fb:.0%}). "
                      f"Al final del horizonte p50 {final.base_pct_llenado_p50:.1f}% "
                      f"(p10–p90 {final.base_pct_llenado_p10:.1f}%–{final.base_pct_llenado_p90:.1f}%).")
    if alerta:
        lineas.append(f"Alerta vigente: **{alerta['nivel_alerta']}**.")
    lineas += notas
    lineas.append(f"_{config.NOTA_MODELO}_")

    parametros = {
        "serie_id": res.serie_id, "id_instalacion": res.id_instalacion, "id_producto": res.id_producto,
        "fecha_corte": base.fecha_corte.iloc[0], "version_modelo": base.version_modelo.iloc[0],
        "fecha_inicio": det.fecha.iloc[0], "fecha_fin": det.fecha.iloc[-1], "horizonte_dias": len(det),
        "cambio": cambio, "frac_base_promedio": round(fb, 4), "frac_escenario_promedio": round(fe, 4),
    }
    return ResultadoEscenario(texto="\n\n".join(lineas), parametros=parametros,
                              detalle=det.round(4).to_dict("records"), resumen=resumen)
