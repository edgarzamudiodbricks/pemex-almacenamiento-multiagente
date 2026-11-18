"""Bronze: ingesta incremental de telemetría diaria de tanques (JSON) con Auto Loader."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
LANDING = f"/Volumes/{CATALOG}/pemex_alm_bronze/landing"

# Tipos conocidos fijados con hints; columnas nuevas se agregan (addNewColumns) y
# valores con tipo inesperado quedan en _rescued_data (no se pierden).
SCHEMA_HINTS = (
    "fecha_lectura STRING, id_tanque STRING, volumen_bbl DOUBLE, recibos_bbl DOUBLE, "
    "despachos_bbl DOUBLE, temperatura_c DOUBLE, fuente STRING, ts_registro TIMESTAMP, carga STRING"
)


@dp.table(
    name=f"{CATALOG}.pemex_alm_bronze.telemetria_inventario",
    comment=(
        "Bronze. Lecturas diarias crudas de inventario por tanque (estilo SCADA) ingestadas "
        "incrementalmente desde el volume landing. Append-only: puede contener duplicados y reprocesos."
    ),
    cluster_by_auto=True,
)
def telemetria_inventario():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.partitionColumns", "carga")  # carpeta carga=<etiqueta>
        .load(f"{LANDING}/telemetria_inventario/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_archivo_origen"),
            F.col("_metadata.file_modification_time").alias("_archivo_modificado_ts"),
            F.current_timestamp().alias("_ingestado_ts"),
        )
    )
