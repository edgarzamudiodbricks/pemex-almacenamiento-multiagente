"""Gold: serie diaria continua (sin huecos) por instalación-producto para entrenar el forecast de capacidad."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
GOLD = f"{CATALOG}.pemex_alm_gold"

METRICAS = {
    "pct_llenado": "% de llenado sobre capacidad operativa (0-100). Variable objetivo principal del forecast.",
    "capacidad_disponible_bbl": "Espacio libre para recibir producto (bbl). Variable objetivo del forecast.",
    "capacidad_operativa_bbl": "Capacidad operativa del día (bbl).",
    "capacidad_nominal_bbl": "Capacidad nominal de tanques vigentes (bbl).",
    "volumen_operativo_bbl": "Volumen en tanques operativos (bbl).",
    "recibos_bbl": "Recibos del día (bbl).",
    "despachos_bbl": "Despachos del día (bbl).",
    "tanques_operativos": "Tanques operativos.",
    "tanques_en_mantenimiento": "Tanques en mantenimiento (regresor exógeno).",
}


def col_c(expr, nombre: str, comentario: str):
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


@dp.materialized_view(
    name=f"{GOLD}.serie_forecast_instalacion_producto",
    comment=(
        "Gold. Serie diaria continua por instalación y producto (una fila por día desde el primer dato hasta la "
        "fecha de corte, sin huecos) con % de llenado y capacidad disponible, lista para entrenar modelos de "
        "pronóstico de capacidad. Días faltantes se rellenan con el último valor (es_imputado = true)."
    ),
    cluster_by=["serie_id", "fecha"],
)
@dp.expect("pct_llenado_en_rango", "pct_llenado IS NULL OR pct_llenado BETWEEN 0 AND 100")
def serie_forecast_instalacion_producto():
    g = spark.read.table(f"{GOLD}.inventario_diario_instalacion_producto")
    atributos = ["id_instalacion", "nombre_instalacion", "tipo_instalacion", "region", "id_producto",
                 "nombre_producto", "categoria_producto"]
    f_max = g.agg(F.max("fecha").alias("f_max"))
    calendario = (
        g.groupBy(*atributos).agg(F.min("fecha").alias("f_ini"))
        .crossJoin(f_max)
        .select(*atributos, F.explode(F.sequence("f_ini", "f_max")).alias("fecha"))
    )
    datos = g.select("id_instalacion", "id_producto", "fecha", *METRICAS.keys()).withColumn("_existe", F.lit(True))
    w = (Window.partitionBy("id_instalacion", "id_producto").orderBy("fecha")
         .rowsBetween(Window.unboundedPreceding, 0))
    df = calendario.join(datos, ["id_instalacion", "id_producto", "fecha"], "left")
    existe = F.coalesce(F.col("_existe"), F.lit(False))
    rellenas = [
        col_c(F.when(existe, F.col(m)).otherwise(F.last(F.when(existe, F.col(m)), ignorenulls=True).over(w)), m, d)
        for m, d in METRICAS.items()
    ]
    return df.select(
        col_c(F.concat_ws("|", "id_instalacion", "id_producto"), "serie_id", "Identificador de la serie: id_instalacion|id_producto."),
        col_c("fecha", "fecha", "Fecha del día (sin huecos)."),
        *atributos,
        *rellenas,
        col_c(~existe, "es_imputado", "True si el día no tenía datos y se rellenó con el último valor."),
        col_c(F.dayofweek("fecha"), "dia_semana", "Día de la semana (1=domingo ... 7=sábado)."),
        col_c(F.month("fecha"), "mes_num", "Mes del año (1-12)."),
        col_c(F.year("fecha"), "anio", "Año."),
    )
