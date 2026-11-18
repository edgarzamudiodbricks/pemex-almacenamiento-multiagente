"""Gold: inventario diario por instalación y producto (base del dashboard, Genie y forecast)."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
SILVER = f"{CATALOG}.pemex_alm_silver"
GOLD = f"{CATALOG}.pemex_alm_gold"


def col_c(expr, nombre: str, comentario: str):
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


def _suma(c: str):
    return F.round(F.sum(F.coalesce(F.col(c), F.lit(0.0))), 1)


@dp.materialized_view(
    name=f"{GOLD}.inventario_diario_instalacion_producto",
    comment=(
        "Gold. Inventario diario por instalación y producto: capacidad nominal y operativa, volumen, % de llenado "
        "sobre capacidad operativa, capacidad disponible (espacio libre), recibos, despachos, días de autonomía "
        "(volumen útil / despacho promedio 7 y 30 días) y conteo de tanques operativos y en mantenimiento. "
        "Unidades en barriles (bbl)."
    ),
    cluster_by=["fecha", "id_instalacion"],
)
@dp.expect("pct_llenado_en_rango", "pct_llenado IS NULL OR pct_llenado BETWEEN 0 AND 100")
def inventario_diario_instalacion_producto():
    f = spark.read.table(f"{SILVER}.fact_inventario_tanque_diario").where("estatus_operativo <> 'FUERA_DE_SERVICIO'")
    inst = spark.read.table(f"{SILVER}.dim_instalacion")
    prod = spark.read.table(f"{SILVER}.dim_producto")

    agg = f.groupBy("fecha", "id_instalacion", "id_producto").agg(
        _suma("capacidad_nominal_bbl").alias("capacidad_nominal_bbl"),
        _suma("capacidad_operativa_bbl").alias("capacidad_operativa_bbl"),
        _suma("volumen_bbl").alias("volumen_bbl"),
        _suma("volumen_operativo_bbl").alias("volumen_operativo_bbl"),
        _suma("volumen_util_bbl").alias("volumen_util_bbl"),
        _suma("capacidad_disponible_bbl").alias("capacidad_disponible_bbl"),
        _suma("recibos_bbl").alias("recibos_bbl"),
        _suma("despachos_bbl").alias("despachos_bbl"),
        F.count("*").alias("tanques_vigentes"),
        F.sum(F.when(F.col("estatus_operativo") == "OPERATIVO", 1).otherwise(0)).alias("tanques_operativos"),
        F.sum(F.when(F.col("estatus_operativo") == "EN_MANTENIMIENTO", 1).otherwise(0)).alias("tanques_en_mantenimiento"),
        F.sum(F.when(F.col("es_imputado"), 1).otherwise(0)).alias("lecturas_imputadas"),
    )
    serie = Window.partitionBy("id_instalacion", "id_producto").orderBy("fecha")
    d7 = F.avg("despachos_bbl").over(serie.rowsBetween(-6, 0))
    d30 = F.avg("despachos_bbl").over(serie.rowsBetween(-29, 0))
    df = (agg.withColumn("despacho_prom_7d", F.round(d7, 1))
             .withColumn("despacho_prom_30d", F.round(d30, 1))
             .join(F.broadcast(inst), "id_instalacion")
             .join(F.broadcast(prod), "id_producto"))

    return df.select(
        col_c("fecha", "fecha", "Fecha del día operativo."),
        col_c(F.trunc("fecha", "month"), "mes", "Primer día del mes de la fecha."),
        col_c("id_instalacion", "id_instalacion", "Clave de la instalación."),
        col_c("nombre_instalacion", "nombre_instalacion", "Nombre de la instalación."),
        col_c("tipo_instalacion", "tipo_instalacion", "REFINERIA, TERMINAL_CRUDO, TERMINAL_MARITIMA o TAD."),
        col_c("region", "region", "Región operativa."),
        col_c("estado", "estado", "Entidad federativa."),
        col_c("id_producto", "id_producto", "Clave del producto."),
        col_c("nombre_producto", "nombre_producto", "Nombre del producto."),
        col_c("categoria", "categoria_producto", "Categoría del producto: CRUDO, REFINADO o GAS."),
        col_c("capacidad_nominal_bbl", "capacidad_nominal_bbl", "Suma de capacidad nominal de tanques vigentes (bbl)."),
        col_c("capacidad_operativa_bbl", "capacidad_operativa_bbl", "Capacidad de tanques OPERATIVOS ese día (excluye mantenimiento) (bbl)."),
        col_c("volumen_bbl", "volumen_bbl", "Volumen físico total almacenado, incluye tanques en mantenimiento (bbl)."),
        col_c("volumen_operativo_bbl", "volumen_operativo_bbl", "Volumen almacenado en tanques operativos (bbl)."),
        col_c("volumen_util_bbl", "volumen_util_bbl", "Volumen útil despachable (volumen operativo - fondaje) (bbl)."),
        col_c("capacidad_disponible_bbl", "capacidad_disponible_bbl", "Espacio libre para recibir producto (capacidad operativa - volumen operativo) (bbl)."),
        col_c(F.when(F.col("capacidad_operativa_bbl") > 0,
                     F.round(F.col("volumen_operativo_bbl") / F.col("capacidad_operativa_bbl") * 100, 2)),
              "pct_llenado", "Porcentaje de llenado = volumen operativo / capacidad operativa x 100 (0-100)."),
        col_c("recibos_bbl", "recibos_bbl", "Volumen recibido en el día (bbl)."),
        col_c("despachos_bbl", "despachos_bbl", "Volumen despachado en el día (bbl)."),
        col_c("despacho_prom_7d", "despacho_prom_7d_bbl", "Despacho diario promedio de los últimos 7 días (bbl/día)."),
        col_c("despacho_prom_30d", "despacho_prom_30d_bbl", "Despacho diario promedio de los últimos 30 días (bbl/día)."),
        col_c(F.when(F.col("despacho_prom_7d") > 0, F.round(F.col("volumen_util_bbl") / F.col("despacho_prom_7d"), 1)),
              "dias_autonomia_7d", "Días de autonomía = volumen útil / despacho promedio 7 días."),
        col_c(F.when(F.col("despacho_prom_30d") > 0, F.round(F.col("volumen_util_bbl") / F.col("despacho_prom_30d"), 1)),
              "dias_autonomia_30d", "Días de autonomía = volumen útil / despacho promedio 30 días."),
        col_c("tanques_vigentes", "tanques_vigentes", "Número de tanques vigentes (operativos + en mantenimiento)."),
        col_c("tanques_operativos", "tanques_operativos", "Número de tanques operativos."),
        col_c("tanques_en_mantenimiento", "tanques_en_mantenimiento", "Número de tanques en mantenimiento."),
        col_c("lecturas_imputadas", "lecturas_imputadas", "Tanques cuyo volumen se imputó por falta de lectura válida."),
    )
