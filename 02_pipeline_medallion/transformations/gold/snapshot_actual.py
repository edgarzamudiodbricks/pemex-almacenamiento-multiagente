"""Gold: snapshot del último día disponible por tanque y por instalación-producto, con semáforo."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
SILVER = f"{CATALOG}.pemex_alm_silver"
GOLD = f"{CATALOG}.pemex_alm_gold"

SEMAFORO_DESC = (
    "Semáforo de llenado: CRITICO_BAJO (<15%), BAJO (15-30%), NORMAL (30-90%), ALTO (>90%); "
    "SIN_CAPACIDAD_OPERATIVA si no hay capacidad operativa; para tanques también EN_MANTENIMIENTO o FUERA_DE_SERVICIO."
)


def col_c(expr, nombre: str, comentario: str):
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


def semaforo(pct):
    return (F.when(pct.isNull(), "SIN_CAPACIDAD_OPERATIVA")
             .when(pct < 15, "CRITICO_BAJO")
             .when(pct < 30, "BAJO")
             .when(pct <= 90, "NORMAL")
             .otherwise("ALTO"))


@dp.materialized_view(
    name=f"{GOLD}.snapshot_tanque_actual",
    comment="Gold. Estado de cada tanque en el último día con datos: volumen, capacidad, % de llenado y semáforo.",
)
def snapshot_tanque_actual():
    f = spark.read.table(f"{SILVER}.fact_inventario_tanque_diario")
    ultimo = f.agg(F.max("fecha").alias("fecha"))
    df = (f.join(ultimo, "fecha")
           .join(spark.read.table(f"{SILVER}.dim_instalacion"), "id_instalacion")
           .join(spark.read.table(f"{SILVER}.dim_producto"), "id_producto"))
    sem = (F.when(F.col("estatus_operativo") != "OPERATIVO", F.col("estatus_operativo"))
            .otherwise(semaforo(F.col("pct_llenado"))))
    return df.select(
        col_c("fecha", "fecha_corte", "Último día con datos de telemetría."),
        col_c("id_tanque", "id_tanque", "Clave del tanque."),
        col_c("id_instalacion", "id_instalacion", "Clave de la instalación."),
        col_c("nombre_instalacion", "nombre_instalacion", "Nombre de la instalación."),
        col_c("tipo_instalacion", "tipo_instalacion", "REFINERIA, TERMINAL_CRUDO, TERMINAL_MARITIMA o TAD."),
        col_c("region", "region", "Región operativa."),
        col_c("estado", "estado", "Entidad federativa."),
        col_c("latitud", "latitud", "Latitud de la instalación."),
        col_c("longitud", "longitud", "Longitud de la instalación."),
        col_c("id_producto", "id_producto", "Clave del producto."),
        col_c("nombre_producto", "nombre_producto", "Nombre del producto."),
        col_c("categoria", "categoria_producto", "CRUDO, REFINADO o GAS."),
        col_c("estatus_operativo", "estatus_operativo", "OPERATIVO, EN_MANTENIMIENTO o FUERA_DE_SERVICIO."),
        col_c("capacidad_nominal_bbl", "capacidad_nominal_bbl", "Capacidad nominal (bbl)."),
        col_c("capacidad_operativa_bbl", "capacidad_operativa_bbl", "Capacidad operativa del día (bbl)."),
        col_c("volumen_bbl", "volumen_bbl", "Volumen físico (bbl)."),
        col_c("volumen_util_bbl", "volumen_util_bbl", "Volumen útil despachable (bbl)."),
        col_c("capacidad_disponible_bbl", "capacidad_disponible_bbl", "Espacio libre (bbl)."),
        col_c("pct_llenado", "pct_llenado", "% de llenado sobre capacidad operativa (0-100)."),
        col_c(sem, "semaforo_llenado", SEMAFORO_DESC),
        col_c("es_imputado", "es_imputado", "True si el volumen se arrastró de la última lectura válida."),
    )


@dp.materialized_view(
    name=f"{GOLD}.snapshot_instalacion_actual",
    comment=(
        "Gold. Estado de cada instalación y producto en el último día con datos: capacidad, volumen, "
        "% de llenado, capacidad disponible, días de autonomía y semáforo."
    ),
)
def snapshot_instalacion_actual():
    g = spark.read.table(f"{GOLD}.inventario_diario_instalacion_producto")
    ultimo = g.agg(F.max("fecha").alias("fecha"))
    df = g.join(ultimo, "fecha").join(
        spark.read.table(f"{SILVER}.dim_instalacion").select("id_instalacion", "latitud", "longitud"), "id_instalacion"
    )
    return df.select(
        col_c("fecha", "fecha_corte", "Último día con datos de telemetría."),
        "id_instalacion", "nombre_instalacion", "tipo_instalacion", "region", "estado",
        col_c("latitud", "latitud", "Latitud de la instalación."),
        col_c("longitud", "longitud", "Longitud de la instalación."),
        "id_producto", "nombre_producto", "categoria_producto",
        "capacidad_nominal_bbl", "capacidad_operativa_bbl", "volumen_bbl", "volumen_operativo_bbl", "volumen_util_bbl",
        "capacidad_disponible_bbl", "pct_llenado", "despacho_prom_7d_bbl", "despacho_prom_30d_bbl",
        "dias_autonomia_7d", "dias_autonomia_30d", "tanques_vigentes", "tanques_operativos", "tanques_en_mantenimiento",
        col_c(semaforo(F.col("pct_llenado")), "semaforo_llenado", SEMAFORO_DESC),
    )
