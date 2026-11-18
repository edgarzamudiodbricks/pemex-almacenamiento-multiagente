"""Bronze: catálogos (snapshots CSV completos) cargados tal cual.

El generador sobrescribe cada catálogo completo en cada corrida, por eso se modelan como
materialized views (recalculadas desde el snapshot vigente) y no como append.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
LANDING = f"/Volumes/{CATALOG}/pemex_alm_bronze/landing"


def _leer_catalogo(nombre: str):
    """Lee el snapshot CSV vigente; todas las columnas como STRING (tipado en Silver)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{LANDING}/catalogos/{nombre}/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_archivo_origen"),
            F.current_timestamp().alias("_ingestado_ts"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.pemex_alm_bronze.productos",
    comment="Bronze. Snapshot crudo del catálogo de productos (CSV).",
)
def productos():
    return _leer_catalogo("productos")


@dp.materialized_view(
    name=f"{CATALOG}.pemex_alm_bronze.instalaciones",
    comment="Bronze. Snapshot crudo del catálogo de instalaciones (CSV).",
)
def instalaciones():
    return _leer_catalogo("instalaciones")


@dp.materialized_view(
    name=f"{CATALOG}.pemex_alm_bronze.tanques",
    comment="Bronze. Snapshot crudo del catálogo de tanques (CSV).",
)
def tanques():
    return _leer_catalogo("tanques")


@dp.materialized_view(
    name=f"{CATALOG}.pemex_alm_bronze.mantenimientos",
    comment="Bronze. Snapshot crudo de ventanas de mantenimiento de tanques (CSV).",
)
def mantenimientos():
    return _leer_catalogo("mantenimientos")
