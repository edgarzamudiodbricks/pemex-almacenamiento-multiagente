"""Gold: resumen mensual por región, tipo de instalación y producto."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
GOLD = f"{CATALOG}.pemex_alm_gold"


def col_c(expr, nombre: str, comentario: str):
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


@dp.materialized_view(
    name=f"{GOLD}.resumen_mensual_region_producto",
    comment=(
        "Gold. Resumen mensual por región, tipo de instalación y producto: promedios diarios de capacidad, "
        "volumen y capacidad disponible, % de llenado promedio/mínimo/máximo, y recibos/despachos totales del mes (bbl)."
    ),
)
def resumen_mensual_region_producto():
    g = spark.read.table(f"{GOLD}.inventario_diario_instalacion_producto")
    llaves = ["mes", "region", "tipo_instalacion", "id_producto", "nombre_producto", "categoria_producto"]

    # 1) agregado diario del grupo (suma de instalaciones)
    diario = g.groupBy("fecha", *llaves).agg(
        F.sum("capacidad_nominal_bbl").alias("cap_nom"),
        F.sum("capacidad_operativa_bbl").alias("cap_op"),
        F.sum("volumen_operativo_bbl").alias("vol_op"),
        F.sum("capacidad_disponible_bbl").alias("cap_disp"),
        F.sum("recibos_bbl").alias("recibos"),
        F.sum("despachos_bbl").alias("despachos"),
        F.countDistinct("id_instalacion").alias("n_inst"),
        F.sum("tanques_operativos").alias("t_op"),
        F.sum("tanques_en_mantenimiento").alias("t_mt"),
    ).withColumn("pct_dia", F.when(F.col("cap_op") > 0, F.col("vol_op") / F.col("cap_op") * 100))

    # 2) agregado mensual
    m = diario.groupBy(*llaves).agg(
        F.round(F.avg("cap_nom"), 1).alias("capacidad_nominal_prom_bbl"),
        F.round(F.avg("cap_op"), 1).alias("capacidad_operativa_prom_bbl"),
        F.round(F.avg("vol_op"), 1).alias("volumen_operativo_prom_bbl"),
        F.round(F.avg("cap_disp"), 1).alias("capacidad_disponible_prom_bbl"),
        F.round(F.sum("vol_op") / F.sum("cap_op") * 100, 2).alias("pct_llenado_prom"),
        F.round(F.min("pct_dia"), 2).alias("pct_llenado_min"),
        F.round(F.max("pct_dia"), 2).alias("pct_llenado_max"),
        F.round(F.sum("recibos"), 1).alias("recibos_total_bbl"),
        F.round(F.sum("despachos"), 1).alias("despachos_total_bbl"),
        F.max("n_inst").alias("instalaciones"),
        F.round(F.avg("t_op"), 1).alias("tanques_operativos_prom"),
        F.round(F.avg("t_mt"), 1).alias("tanques_en_mantenimiento_prom"),
        F.count("*").alias("dias_con_datos"),
    )
    desc = {
        "mes": "Primer día del mes.",
        "region": "Región operativa.",
        "tipo_instalacion": "REFINERIA, TERMINAL_CRUDO, TERMINAL_MARITIMA o TAD.",
        "id_producto": "Clave del producto.",
        "nombre_producto": "Nombre del producto.",
        "categoria_producto": "CRUDO, REFINADO o GAS.",
        "capacidad_nominal_prom_bbl": "Promedio diario de capacidad nominal en el mes (bbl).",
        "capacidad_operativa_prom_bbl": "Promedio diario de capacidad operativa en el mes (bbl).",
        "volumen_operativo_prom_bbl": "Promedio diario de volumen en tanques operativos (bbl).",
        "capacidad_disponible_prom_bbl": "Promedio diario de espacio libre (bbl).",
        "pct_llenado_prom": "% de llenado promedio del mes (volumen operativo / capacidad operativa).",
        "pct_llenado_min": "% de llenado diario mínimo del mes.",
        "pct_llenado_max": "% de llenado diario máximo del mes.",
        "recibos_total_bbl": "Total recibido en el mes (bbl).",
        "despachos_total_bbl": "Total despachado en el mes (bbl).",
        "instalaciones": "Número de instalaciones con el producto en el grupo.",
        "tanques_operativos_prom": "Promedio diario de tanques operativos.",
        "tanques_en_mantenimiento_prom": "Promedio diario de tanques en mantenimiento.",
        "dias_con_datos": "Días del mes con datos.",
    }
    return m.select(*[col_c(c, c, d) for c, d in desc.items()])
