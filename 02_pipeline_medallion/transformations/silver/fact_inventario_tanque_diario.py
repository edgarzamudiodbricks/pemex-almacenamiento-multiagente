"""Silver: hecho diario por tanque, sin huecos de fecha, con estatus operativo y capacidad del día.

Se construye una rejilla tanque x día (vigencia del tanque dentro del rango de telemetría) y se
le une la lectura depurada. Si falta la lectura de un día (p.ej. quedó en cuarentena) el volumen
se imputa con la última lectura válida (es_imputado = true) y los flujos quedan en 0.
Al ser MV, refleja siempre la versión vigente de catálogos y mantenimientos (reprocesable).
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
SILVER = f"{CATALOG}.pemex_alm_silver"


def col_c(expr, nombre: str, comentario: str):
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


@dp.materialized_view(
    name=f"{SILVER}.fact_inventario_tanque_diario",
    comment=(
        "Silver. Inventario diario por tanque (una fila por id_tanque y fecha, sin huecos dentro de la vigencia). "
        "Incluye estatus operativo (OPERATIVO, EN_MANTENIMIENTO, FUERA_DE_SERVICIO), capacidad operativa del día "
        "(0 si en mantenimiento o fuera de vigencia), volumen, % de llenado y flujos de recibo/despacho."
    ),
    cluster_by=["fecha", "id_instalacion"],
)
@dp.expect("pct_llenado_en_rango", "pct_llenado IS NULL OR pct_llenado BETWEEN 0 AND 100")
@dp.expect("volumen_disponible", "volumen_bbl IS NOT NULL")
def fact_inventario_tanque_diario():
    lect = spark.read.table(f"{SILVER}.telemetria_depurada")
    tq = spark.read.table(f"{SILVER}.dim_tanque")
    mt_dias = (
        spark.read.table(f"{SILVER}.mantenimientos")
        .select("id_tanque", "tipo_mantenimiento",
                F.explode(F.sequence("fecha_inicio", "fecha_fin")).alias("fecha"))
        .groupBy("id_tanque", "fecha")
        .agg(F.min("tipo_mantenimiento").alias("tipo_mantenimiento"))
    )
    rango = lect.agg(F.min("fecha_lectura").alias("f_min"), F.max("fecha_lectura").alias("f_max"))

    # Rejilla tanque x día dentro de la vigencia (fecha_baja exclusiva) acotada al rango de telemetría
    rejilla = (
        tq.crossJoin(rango)
        .select(
            "id_tanque",
            F.greatest("fecha_alta", "f_min").alias("ini"),
            F.least(F.coalesce(F.date_sub("fecha_baja", 1), "f_max"), "f_max").alias("fin"),
        )
        .where("ini <= fin")
        .select("id_tanque", F.explode(F.sequence("ini", "fin")).alias("fecha"))
    )
    llaves = rejilla.unionByName(
        lect.select("id_tanque", F.col("fecha_lectura").alias("fecha"))
    ).distinct()

    w_prev = Window.partitionBy("id_tanque").orderBy("fecha").rowsBetween(Window.unboundedPreceding, -1)
    df = (
        llaves.join(lect.withColumnRenamed("fecha_lectura", "fecha"), ["id_tanque", "fecha"], "left")
        .join(tq, "id_tanque", "inner")
        .join(mt_dias, ["id_tanque", "fecha"], "left")
        .withColumn("es_imputado", F.col("volumen_bbl").isNull())
        .withColumn("volumen_bbl", F.coalesce("volumen_bbl", F.last("volumen_bbl", ignorenulls=True).over(w_prev)))
        .withColumn("en_vigencia", (F.col("fecha") >= F.col("fecha_alta"))
                    & (F.col("fecha_baja").isNull() | (F.col("fecha") < F.col("fecha_baja"))))
        .withColumn("en_mantenimiento", F.col("tipo_mantenimiento").isNotNull())
        .withColumn("estatus_operativo",
                    F.when(~F.col("en_vigencia"), "FUERA_DE_SERVICIO")
                    .when(F.col("en_mantenimiento"), "EN_MANTENIMIENTO")
                    .otherwise("OPERATIVO"))
    )
    operativo = F.col("estatus_operativo") == "OPERATIVO"
    cap_op = F.when(operativo, F.col("capacidad_nominal_bbl")).otherwise(F.lit(0.0))
    vol_op = F.when(operativo, F.coalesce("volumen_bbl", F.lit(0.0))).otherwise(F.lit(0.0))

    return df.select(
        col_c("fecha", "fecha", "Fecha de la lectura (día operativo)."),
        col_c("id_tanque", "id_tanque", "Clave del tanque."),
        col_c("id_instalacion", "id_instalacion", "Instalación del tanque."),
        col_c("id_producto", "id_producto", "Producto almacenado."),
        col_c("estatus_operativo", "estatus_operativo", "OPERATIVO, EN_MANTENIMIENTO o FUERA_DE_SERVICIO (fuera de fecha_alta/fecha_baja)."),
        col_c("en_mantenimiento", "en_mantenimiento", "True si la fecha cae dentro de una ventana de mantenimiento."),
        col_c("tipo_mantenimiento", "tipo_mantenimiento", "Tipo de mantenimiento activo ese día (nulo si no aplica)."),
        col_c("capacidad_nominal_bbl", "capacidad_nominal_bbl", "Capacidad nominal del tanque (barriles)."),
        col_c("fondaje_bbl", "fondaje_bbl", "Fondaje no bombeable (barriles)."),
        col_c(cap_op, "capacidad_operativa_bbl", "Capacidad operativa del día: nominal si OPERATIVO, 0 en otro caso (barriles)."),
        col_c(F.when(operativo, F.col("capacidad_util_bbl")).otherwise(F.lit(0.0)), "capacidad_util_operativa_bbl",
              "Capacidad útil operativa del día: nominal - fondaje si OPERATIVO, 0 en otro caso (barriles)."),
        col_c("volumen_bbl", "volumen_bbl", "Volumen físico en el tanque (barriles), incluye fondaje."),
        col_c(vol_op, "volumen_operativo_bbl", "Volumen en tanques operativos (0 si el tanque no está OPERATIVO)."),
        col_c(F.greatest(vol_op - F.when(operativo, F.col("fondaje_bbl")).otherwise(F.lit(0.0)), F.lit(0.0)),
              "volumen_util_bbl", "Volumen útil despachable = volumen operativo - fondaje (barriles)."),
        col_c(F.greatest(cap_op - vol_op, F.lit(0.0)), "capacidad_disponible_bbl",
              "Espacio libre para recibir producto = capacidad operativa - volumen operativo (barriles)."),
        col_c(F.when(cap_op > 0, F.round(vol_op / cap_op * 100, 2)), "pct_llenado",
              "Porcentaje de llenado sobre capacidad operativa (0-100). Nulo si capacidad operativa = 0."),
        col_c(F.coalesce("recibos_bbl", F.lit(0.0)), "recibos_bbl", "Volumen recibido en el día (barriles)."),
        col_c(F.coalesce("despachos_bbl", F.lit(0.0)), "despachos_bbl", "Volumen despachado en el día (barriles)."),
        col_c("temperatura_c", "temperatura_c", "Temperatura del producto (°C)."),
        col_c("es_imputado", "es_imputado", "True si no hubo lectura válida ese día y el volumen se arrastró de la última lectura."),
        col_c("ts_registro", "ts_registro", "Timestamp de registro de la lectura en origen."),
    )
