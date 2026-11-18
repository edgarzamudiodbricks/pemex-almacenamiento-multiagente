"""Silver: validación, cuarentena y deduplicación de la telemetría diaria por tanque.

Flujo:
  bronze.telemetria_inventario (append, con duplicados/reprocesos)
    -> telemetria_evaluada (vista: tipado + reglas de calidad + motivo_rechazo)
        -> silver.cuarentena_telemetria (registros rechazados, append)
        -> telemetria_valida (vista con expectativas DROP = métricas de calidad)
            -> silver.telemetria_depurada (Auto CDC SCD1: 1 fila por id_tanque+fecha_lectura,
               gana el registro más reciente por ts_registro / archivo)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
BRONZE = f"{CATALOG}.pemex_alm_bronze"
SILVER = f"{CATALOG}.pemex_alm_silver"

# Reglas de calidad: nombre -> condición que DEBE cumplirse
REGLAS = {
    "fecha_lectura_valida": "fecha_lectura IS NOT NULL",
    "tanque_existente": "tanque_existe",
    "volumen_no_nulo": "volumen_bbl IS NOT NULL",
    "volumen_no_negativo": "volumen_bbl >= 0",
    "volumen_dentro_capacidad": "volumen_bbl <= capacidad_nominal_bbl",
}


@dp.temporary_view(comment="Telemetría tipada con la evaluación de reglas de calidad por registro.")
def telemetria_evaluada():
    tanques = spark.read.table(f"{SILVER}.dim_tanque").select(
        "id_tanque", "capacidad_nominal_bbl", F.lit(True).alias("tanque_existe")
    )
    df = (
        spark.readStream.table(f"{BRONZE}.telemetria_inventario")
        .select(
            F.expr("try_to_date(fecha_lectura, 'yyyy-MM-dd')").alias("fecha_lectura"),
            F.trim("id_tanque").alias("id_tanque"),
            F.col("volumen_bbl").cast("double").alias("volumen_bbl"),
            F.col("recibos_bbl").cast("double").alias("recibos_bbl"),
            F.col("despachos_bbl").cast("double").alias("despachos_bbl"),
            F.col("temperatura_c").cast("double").alias("temperatura_c"),
            "fuente",
            "ts_registro",
            "carga",
            "_archivo_origen",
            "_archivo_modificado_ts",
            "_ingestado_ts",
            F.col("fecha_lectura").alias("fecha_lectura_original"),
        )
        .join(F.broadcast(tanques), "id_tanque", "left")  # stream-static join
        .withColumn("tanque_existe", F.coalesce("tanque_existe", F.lit(False)))
    )
    # Lista de reglas incumplidas (NULL en la condición cuenta como incumplida)
    fallas = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(nombre)) for nombre, cond in REGLAS.items()]
    return df.withColumn("motivo_rechazo", F.concat_ws(",", *fallas)).withColumn(
        "es_valido", F.col("motivo_rechazo") == ""
    )


@dp.table(
    name=f"{SILVER}.cuarentena_telemetria",
    comment=(
        "Silver. Cuarentena de lecturas de telemetría rechazadas por reglas de calidad "
        "(volumen nulo/negativo, volumen > capacidad nominal, tanque inexistente, fecha inválida). "
        "motivo_rechazo lista las reglas incumplidas separadas por coma."
    ),
)
def cuarentena_telemetria():
    return spark.readStream.table("telemetria_evaluada").where(~F.col("es_valido")).drop("es_valido")


@dp.temporary_view(comment="Telemetría que cumple todas las reglas (las expectativas registran métricas).")
@dp.expect_all_or_drop(REGLAS)
def telemetria_valida():
    return spark.readStream.table("telemetria_evaluada")


dp.create_streaming_table(
    name=f"{SILVER}.telemetria_depurada",
    comment=(
        "Silver. Lectura diaria válida y deduplicada por tanque: una fila por (id_tanque, fecha_lectura). "
        "Ante reprocesos o duplicados se conserva el registro más reciente (ts_registro, archivo)."
    ),
    cluster_by=["id_tanque", "fecha_lectura"],
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.telemetria_depurada",
    source="telemetria_valida",
    keys=["id_tanque", "fecha_lectura"],
    sequence_by=F.struct("ts_registro", "_archivo_modificado_ts"),
    stored_as_scd_type=1,
    column_list=[
        "id_tanque", "fecha_lectura", "volumen_bbl", "recibos_bbl", "despachos_bbl", "temperatura_c",
        "fuente", "ts_registro", "carga", "_archivo_origen", "_archivo_modificado_ts", "_ingestado_ts",
    ],
)
