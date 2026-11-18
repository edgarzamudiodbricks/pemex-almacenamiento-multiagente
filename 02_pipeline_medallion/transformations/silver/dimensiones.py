"""Silver: dimensiones limpias y tipadas a partir de los snapshots de catálogo en Bronze."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
BRONZE = f"{CATALOG}.pemex_alm_bronze"
SILVER = f"{CATALOG}.pemex_alm_silver"


def col_c(expr, nombre: str, comentario: str):
    """Alias con comentario de columna (se publica en Unity Catalog para Genie)."""
    return (F.col(expr) if isinstance(expr, str) else expr).alias(nombre, metadata={"comment": comentario})


@dp.materialized_view(
    name=f"{SILVER}.dim_producto",
    comment="Silver. Catálogo de productos almacenados (crudos, refinados y gas LP).",
)
@dp.expect_or_drop("id_producto_no_nulo", "id_producto IS NOT NULL")
def dim_producto():
    return spark.read.table(f"{BRONZE}.productos").select(
        col_c(F.trim("id_producto"), "id_producto", "Clave del producto (p.ej. GAS-REG, DIE, CRU-MAYA)."),
        col_c(F.trim("nombre_producto"), "nombre_producto", "Nombre comercial del producto."),
        col_c(F.upper(F.trim("categoria")), "categoria", "Categoría: CRUDO, REFINADO o GAS."),
        col_c(F.col("grados_api").cast("double"), "grados_api", "Gravedad API (solo crudos)."),
        col_c(F.col("densidad_kg_m3").cast("double"), "densidad_kg_m3", "Densidad aproximada en kg/m3."),
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_instalacion",
    comment="Silver. Instalaciones de almacenamiento de PEMEX: refinerías, terminales de crudo, terminales marítimas y TAD.",
)
@dp.expect_or_drop("id_instalacion_no_nulo", "id_instalacion IS NOT NULL")
def dim_instalacion():
    return spark.read.table(f"{BRONZE}.instalaciones").select(
        col_c(F.trim("id_instalacion"), "id_instalacion", "Clave de la instalación (p.ej. REF-TUL, TAD-AZC)."),
        col_c(F.trim("nombre_instalacion"), "nombre_instalacion", "Nombre de la instalación."),
        col_c(F.upper(F.trim("tipo_instalacion")), "tipo_instalacion",
              "Tipo: REFINERIA, TERMINAL_CRUDO, TERMINAL_MARITIMA o TAD (Terminal de Almacenamiento y Despacho)."),
        col_c(F.trim("estado"), "estado", "Entidad federativa donde se ubica."),
        col_c(F.upper(F.trim("region")), "region", "Región operativa: NORTE, CENTRO, OCCIDENTE, GOLFO, SURESTE, PACIFICO."),
        col_c(F.col("latitud").cast("double"), "latitud", "Latitud (grados decimales)."),
        col_c(F.col("longitud").cast("double"), "longitud", "Longitud (grados decimales)."),
        col_c(F.to_date("fecha_inicio_operacion"), "fecha_inicio_operacion", "Fecha de inicio de operación."),
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_tanque",
    comment="Silver. Tanques de almacenamiento con capacidad nominal, fondaje y capacidad útil (nominal - fondaje).",
)
@dp.expect_or_drop("id_tanque_no_nulo", "id_tanque IS NOT NULL")
@dp.expect_or_drop("capacidad_positiva", "capacidad_nominal_bbl > 0")
@dp.expect("fondaje_menor_capacidad", "fondaje_bbl < capacidad_nominal_bbl")
def dim_tanque():
    t = spark.read.table(f"{BRONZE}.tanques")
    cap = F.col("capacidad_nominal_bbl").cast("double")
    fon = F.coalesce(F.col("fondaje_bbl").cast("double"), F.lit(0.0))
    return t.select(
        col_c(F.trim("id_tanque"), "id_tanque", "Clave del tanque (instalación + número)."),
        col_c(F.trim("id_instalacion"), "id_instalacion", "Instalación a la que pertenece el tanque."),
        col_c(F.trim("id_producto"), "id_producto", "Producto asignado al tanque."),
        col_c(cap, "capacidad_nominal_bbl", "Capacidad nominal del tanque en barriles."),
        col_c(fon, "fondaje_bbl", "Volumen de fondaje (no bombeable) en barriles."),
        col_c(F.greatest(cap - fon, F.lit(0.0)), "capacidad_util_bbl", "Capacidad útil = capacidad nominal - fondaje (barriles)."),
        col_c(F.upper(F.trim("tipo_techo")), "tipo_techo", "Tipo de techo: FLOTANTE, FIJO o ESFERA."),
        col_c(F.to_date("fecha_alta"), "fecha_alta", "Fecha de entrada en operación del tanque."),
        col_c(F.to_date("fecha_baja"), "fecha_baja", "Fecha de baja definitiva (nula si sigue vigente)."),
    )


@dp.materialized_view(
    name=f"{SILVER}.mantenimientos",
    comment="Silver. Ventanas de mantenimiento de tanques (el tanque no tiene capacidad operativa en esas fechas).",
)
@dp.expect_or_drop("ventana_valida", "fecha_inicio IS NOT NULL AND fecha_fin >= fecha_inicio")
def mantenimientos():
    m = spark.read.table(f"{BRONZE}.mantenimientos")
    ini, fin = F.to_date("fecha_inicio"), F.to_date("fecha_fin")
    return m.select(
        col_c(F.trim("id_mantenimiento"), "id_mantenimiento", "Clave del mantenimiento."),
        col_c(F.trim("id_tanque"), "id_tanque", "Tanque intervenido."),
        col_c(ini, "fecha_inicio", "Inicio de la ventana de mantenimiento (inclusive)."),
        col_c(fin, "fecha_fin", "Fin de la ventana de mantenimiento (inclusive)."),
        col_c(F.upper(F.trim("tipo_mantenimiento")), "tipo_mantenimiento",
              "Tipo: PREVENTIVO, CORRECTIVO, INSPECCION_API653 o LIMPIEZA."),
        col_c(F.datediff(fin, ini) + 1, "duracion_dias", "Duración de la ventana en días."),
    )
