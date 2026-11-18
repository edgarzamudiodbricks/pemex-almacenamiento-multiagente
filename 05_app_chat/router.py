"""Router de intencion determinista: 'modelo' (escenario / pronostico de una serie) o 'genie' (datos)."""

import re
from typing import Optional

import pandas as pd

from escenario import normalizar, resolver_serie

PATRONES_ESCENARIO = [
    r"\bque pasa(ria)? si\b", r"\bwhat ?if\b", r"\bescenario", r"\bsimul", r"\bsupon",
    r"\by si\b.*\d+\s*(por ciento)?", r"\bsi (la|el) capacidad",
    r"\bcapacidad operativa\b.*\b(baja|sube|reduce|aumenta|cae|disminuye)",
    r"\b(baja|sube|reduce|aumenta|disminuye|cae)\w*\s+(a|al|en|un)?\s*\d+",
]
PATRONES_PRONOSTICO = [
    r"\bpronostic", r"\bproyecc", r"\bpredic", r"\bforecast", r"\bse va a (llenar|saturar|vaciar)",
    r"\b(proxim|siguient)\w*\s+\d+\s+dias", r"\bp10\b|\bp50\b|\bp90\b",
]


def _coincide(patrones: list[str], texto: str) -> bool:
    return any(re.search(p, texto) for p in patrones)


def rutear(pregunta: str, catalogo: pd.DataFrame, previo: Optional[dict] = None) -> str:
    t = normalizar(pregunta)
    if _coincide(PATRONES_ESCENARIO, t):
        return "modelo"
    # pronostico de una serie concreta -> modelo; rankings/agregados de pronostico -> Genie (tiene alertas)
    if _coincide(PATRONES_PRONOSTICO, t) and resolver_serie(pregunta, catalogo).serie_id:
        return "modelo"
    return "genie"
