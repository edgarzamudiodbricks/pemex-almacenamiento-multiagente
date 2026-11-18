# Databricks notebook source
# DBTITLE 1,Descripción
# MAGIC %md
# MAGIC # PEMEX Almacenamiento - Generador de datos sintéticos
# MAGIC
# MAGIC Genera archivos crudos en `/Volumes/<CATALOG>/pemex_alm_bronze/landing`:
# MAGIC
# MAGIC * `catalogos/{productos,instalaciones,tanques,mantenimientos}` (CSV, overwrite)
# MAGIC * `telemetria_inventario/carga=<etiqueta>/` (JSON, lecturas diarias por tanque, estilo SCADA)
# MAGIC
# MAGIC Instalaciones y ubicaciones basadas en información pública (refinerías del SNR, terminales marítimas, TAD); capacidades, inventarios y flujos son **sintéticos**.
# MAGIC
# MAGIC Modos:
# MAGIC * `backfill`: 2023-01-01 a ayer
# MAGIC * `incremental`: `fecha_inicio`..`fecha_fin` (default: ayer). Valores determinísticos por (tanque, fecha) -> reprocesar es idempotente a nivel Silver (dedupe).
# MAGIC
# MAGIC Ruido intencional para expectativas: ~0.1% volumen nulo, ~0.05% picos >100% capacidad, ~0.1% duplicados.

# COMMAND ----------

# DBTITLE 1,Parámetros
import datetime as dt
import math
import random

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.dropdown("modo", "incremental", ["backfill", "incremental"])
dbutils.widgets.text("fecha_inicio", "")
dbutils.widgets.text("fecha_fin", "")

CATALOG = "<CATALOG>"  # parámetro: ver README
LANDING = f"/Volumes/{CATALOG}/pemex_alm_bronze/landing"
START = dt.date(2023, 1, 1)
AYER = dt.date.today() - dt.timedelta(days=1)

MODO = dbutils.widgets.get("modo")
if MODO == "backfill":
    FECHA_INI, FECHA_FIN = START, AYER
else:
    FECHA_FIN = dt.date.fromisoformat(dbutils.widgets.get("fecha_fin") or AYER.isoformat())
    if dbutils.widgets.get("fecha_inicio"):
        FECHA_INI = dt.date.fromisoformat(dbutils.widgets.get("fecha_inicio"))
    else:
        # Catch-up automático: continúa desde la última fecha ya ingestada en Bronze
        ultima = spark.sql(
            f"SELECT max(to_date(fecha_lectura)) AS f FROM {CATALOG}.pemex_alm_bronze.telemetria_inventario"
        ).first()["f"]
        FECHA_INI = (ultima + dt.timedelta(days=1)) if ultima else FECHA_FIN

if FECHA_INI > FECHA_FIN:
    dbutils.notebook.exit(f"Sin fechas nuevas por generar (última ingestada: {FECHA_INI - dt.timedelta(days=1)})")
print(MODO, FECHA_INI, FECHA_FIN)

# COMMAND ----------

# DBTITLE 1,Catálogos: productos, instalaciones, tanques, mantenimientos
productos = [
 ("CRU-MAYA","Petróleo crudo Maya","CRUDO",21.5,925.0),
 ("CRU-ISTMO","Petróleo crudo Istmo","CRUDO",33.6,857.0),
 ("CRU-OLMECA","Petróleo crudo Olmeca","CRUDO",39.3,828.0),
 ("GAS-REG","Gasolina Regular (<92 oct)","REFINADO",None,740.0),
 ("GAS-PRE","Gasolina Premium (>=92 oct)","REFINADO",None,750.0),
 ("DIE","Diésel automotriz","REFINADO",None,840.0),
 ("TURB","Turbosina (Jet fuel)","REFINADO",None,800.0),
 ("COMB","Combustóleo","REFINADO",None,990.0),
 ("GLP","Gas LP","GAS",None,540.0),
]
productos_schema = "id_producto string, nombre_producto string, categoria string, grados_api double, densidad_kg_m3 double"

# (id, nombre, tipo, estado, region, lat, lon, fecha_inicio_operacion)
inst = [
 ("REF-CAD","Refinería Ing. Héctor R. Lara Sosa (Cadereyta)","REFINERIA","Nuevo León","NORTE",25.58,-99.98,"1979-01-01"),
 ("REF-MAD","Refinería Francisco I. Madero (Ciudad Madero)","REFINERIA","Tamaulipas","GOLFO",22.26,-97.83,"1914-01-01"),
 ("REF-MIN","Refinería Gral. Lázaro Cárdenas (Minatitlán)","REFINERIA","Veracruz","GOLFO",17.99,-94.55,"1906-01-01"),
 ("REF-SAL","Refinería Ing. Antonio M. Amor (Salamanca)","REFINERIA","Guanajuato","CENTRO",20.57,-101.19,"1950-01-01"),
 ("REF-SCR","Refinería Ing. Antonio Dovalí Jaime (Salina Cruz)","REFINERIA","Oaxaca","PACIFICO",16.19,-95.20,"1979-01-01"),
 ("REF-TUL","Refinería Miguel Hidalgo (Tula)","REFINERIA","Hidalgo","CENTRO",20.05,-99.27,"1976-01-01"),
 ("REF-OLM","Refinería Olmeca (Dos Bocas)","REFINERIA","Tabasco","SURESTE",18.42,-93.18,"2024-07-01"),
 ("TMC-DBO","Terminal Marítima Dos Bocas","TERMINAL_CRUDO","Tabasco","SURESTE",18.43,-93.17,"1982-01-01"),
 ("TMC-CAR","Terminal Flotante Cayo Arcas","TERMINAL_CRUDO","Campeche","SURESTE",20.20,-91.97,"1981-01-01"),
 ("TMC-PAJ","Terminal Marítima Pajaritos","TERMINAL_MARITIMA","Veracruz","GOLFO",18.12,-94.40,"1967-01-01"),
 ("TMR-TUX","Terminal Marítima Tuxpan","TERMINAL_MARITIMA","Veracruz","GOLFO",20.96,-97.40,"1960-01-01"),
 ("TMR-VER","Terminal Marítima Veracruz","TERMINAL_MARITIMA","Veracruz","GOLFO",19.20,-96.13,"1960-01-01"),
 ("TMR-PRO","Terminal Marítima Progreso","TERMINAL_MARITIMA","Yucatán","SURESTE",21.28,-89.66,"1970-01-01"),
 ("TMR-SCR","Terminal Marítima Salina Cruz","TERMINAL_MARITIMA","Oaxaca","PACIFICO",16.16,-95.19,"1979-01-01"),
 ("TMR-ACA","Terminal Marítima Acapulco","TERMINAL_MARITIMA","Guerrero","PACIFICO",16.85,-99.88,"1965-01-01"),
 ("TMR-LZC","Terminal Marítima Lázaro Cárdenas","TERMINAL_MARITIMA","Michoacán","PACIFICO",17.95,-102.18,"1985-01-01"),
 ("TMR-MAN","Terminal Marítima Manzanillo","TERMINAL_MARITIMA","Colima","PACIFICO",19.07,-104.30,"1970-01-01"),
 ("TMR-MZT","Terminal Marítima Mazatlán","TERMINAL_MARITIMA","Sinaloa","PACIFICO",23.20,-106.42,"1968-01-01"),
 ("TMR-TOP","Terminal Marítima Topolobampo","TERMINAL_MARITIMA","Sinaloa","PACIFICO",25.60,-109.05,"1975-01-01"),
 ("TMR-GUY","Terminal Marítima Guaymas","TERMINAL_MARITIMA","Sonora","PACIFICO",27.92,-110.89,"1970-01-01"),
 ("TMR-LPZ","Terminal Marítima La Paz","TERMINAL_MARITIMA","Baja California Sur","PACIFICO",24.15,-110.32,"1972-01-01"),
 ("TMR-ROS","Terminal Marítima Rosarito","TERMINAL_MARITIMA","Baja California","NORTE",32.35,-117.06,"1978-01-01"),
]
tad = [("AZC","Azcapotzalco","Ciudad de México","CENTRO",19.49,-99.18),("ANI","Añil","Ciudad de México","CENTRO",19.39,-99.10),
 ("SJI","San Juan Ixhuatepec","Estado de México","CENTRO",19.52,-99.11),("TOL","Toluca","Estado de México","CENTRO",19.29,-99.62),
 ("PUE","Puebla","Puebla","CENTRO",19.04,-98.20),("QRO","Querétaro","Querétaro","CENTRO",20.59,-100.39),
 ("PAC","Pachuca","Hidalgo","CENTRO",20.10,-98.75),("CUE","Cuernavaca","Morelos","CENTRO",18.92,-99.23),
 ("LEO","León","Guanajuato","CENTRO",21.12,-101.68),("AGS","Aguascalientes","Aguascalientes","CENTRO",21.88,-102.29),
 ("SLP","San Luis Potosí","San Luis Potosí","CENTRO",22.15,-100.98),("MOR","Morelia","Michoacán","CENTRO",19.70,-101.19),
 ("GDL","Guadalajara (El Castillo)","Jalisco","OCCIDENTE",20.52,-103.20),("ZAP","Zapopan","Jalisco","OCCIDENTE",20.72,-103.39),
 ("TEP","Tepic","Nayarit","OCCIDENTE",21.50,-104.89),("COL","Colima","Colima","OCCIDENTE",19.24,-103.72),
 ("ZAC","Zacatecas","Zacatecas","OCCIDENTE",22.77,-102.58),("MTY","Santa Catarina (Monterrey)","Nuevo León","NORTE",25.67,-100.45),
 ("SAL","Saltillo","Coahuila","NORTE",25.42,-101.00),("CHI","Chihuahua","Chihuahua","NORTE",28.63,-106.07),
 ("CJU","Ciudad Juárez","Chihuahua","NORTE",31.69,-106.42),("HMO","Hermosillo","Sonora","NORTE",29.07,-110.96),
 ("MXL","Mexicali","Baja California","NORTE",32.62,-115.45),("DGO","Durango","Durango","NORTE",24.02,-104.65),
 ("GPA","Gómez Palacio","Durango","NORTE",25.56,-103.50),("REY","Reynosa","Tamaulipas","NORTE",26.09,-98.28),
 ("NLD","Nuevo Laredo","Tamaulipas","NORTE",27.48,-99.52),("CUL","Culiacán","Sinaloa","NORTE",24.80,-107.39),
 ("XAL","Xalapa","Veracruz","GOLFO",19.54,-96.91),("VHS","Villahermosa","Tabasco","SURESTE",17.99,-92.93),
 ("MER","Mérida","Yucatán","SURESTE",20.97,-89.62),("CAM","Campeche","Campeche","SURESTE",19.83,-90.53),
 ("CUN","Cancún","Quintana Roo","SURESTE",21.16,-86.85),("TGZ","Tuxtla Gutiérrez","Chiapas","SURESTE",16.75,-93.12),
 ("TAP","Tapachula","Chiapas","SURESTE",14.90,-92.26),("OAX","Oaxaca","Oaxaca","PACIFICO",17.07,-96.72),
 ("IGU","Iguala","Guerrero","PACIFICO",18.34,-99.54)]
big_cities = {"AZC","ANI","SJI","GDL","MTY","PUE","QRO","CUN","MER","LEO","HMO","CJU"}
for c, n, e, r, la, lo in tad:
    inst.append((f"TAD-{c}", f"TAD {n}", "TAD", e, r, la, lo, "1975-01-01"))
inst_schema = "id_instalacion string, nombre_instalacion string, tipo_instalacion string, estado string, region string, latitud double, longitud double, fecha_inicio_operacion string"

# Mezcla de tanques por tipo de instalación: (producto, n_tanques, rango capacidad kbbl)
mix = {
 "REFINERIA":[("CRU-MAYA",2,(250,500)),("CRU-ISTMO",2,(200,400)),("GAS-REG",2,(150,300)),("GAS-PRE",1,(100,200)),
              ("DIE",2,(150,300)),("TURB",1,(80,150)),("COMB",2,(150,300))],
 "TERMINAL_CRUDO":[("CRU-MAYA",4,(400,700)),("CRU-ISTMO",2,(300,500)),("CRU-OLMECA",2,(250,450))],
 "TERMINAL_MARITIMA":[("GAS-REG",2,(100,250)),("GAS-PRE",1,(60,150)),("DIE",2,(100,250)),("TURB",1,(50,120)),("COMB",1,(80,200))],
 "TAD":[("GAS-REG",2,(30,90)),("GAS-PRE",1,(15,45)),("DIE",2,(25,80))],
}
rng = random.Random(42)  # semilla fija -> catálogos estables entre corridas
tanques, mttos = [], []
for (iid, _, tipo, _, _, _, _, f_ini) in inst:
    m = list(mix[tipo])
    if tipo == "TAD" and iid[4:] in big_cities: m.append(("TURB", 1, (20, 50)))
    if iid == "TMC-PAJ": m.append(("GLP", 2, (80, 150)))
    if iid == "TMC-CAR": m = [("CRU-MAYA", 3, (1000, 1300))]  # FSO Cayo Arcas
    k = 0
    for prod, n, (lo, hi) in m:
        for _ in range(n):
            k += 1
            cap = round(rng.uniform(lo, hi) * 1000, -2)
            alta = max(dt.date.fromisoformat(f_ini), dt.date(rng.randint(1980, 2015), 1, 1)).isoformat()
            if iid == "REF-OLM": alta = (dt.date(2024, 7, 1) + dt.timedelta(days=rng.randint(0, 240))).isoformat()
            elif rng.random() < 0.06: alta = (dt.date(2023, 6, 1) + dt.timedelta(days=rng.randint(0, 1150))).isoformat()  # expansiones
            baja = (dt.date(2024, 1, 1) + dt.timedelta(days=rng.randint(0, 600))).isoformat() if rng.random() < 0.025 else None
            techo = "FLOTANTE" if prod.startswith(("GAS", "CRU", "TURB")) else ("ESFERA" if prod == "GLP" else "FIJO")
            tid = f"{iid}-TV{k:02d}"
            tanques.append((tid, iid, prod, float(cap), float(round(cap * 0.05, -2)), techo, alta, baja))
            for _ in range(rng.choice([0, 0, 1, 1, 2])):
                ini = dt.date(2023, 1, 15) + dt.timedelta(days=rng.randint(0, 1350))
                fin = ini + dt.timedelta(days=rng.randint(15, 75))
                tipo_m = rng.choice(["PREVENTIVO", "CORRECTIVO", "INSPECCION_API653", "LIMPIEZA"])
                mttos.append((f"MT-{len(mttos)+1:05d}", tid, ini.isoformat(), fin.isoformat(), tipo_m))
tanques_schema = "id_tanque string, id_instalacion string, id_producto string, capacidad_nominal_bbl double, fondaje_bbl double, tipo_techo string, fecha_alta string, fecha_baja string"
mttos_schema = "id_mantenimiento string, id_tanque string, fecha_inicio string, fecha_fin string, tipo_mantenimiento string"
print(len(inst), "instalaciones |", len(tanques), "tanques |", len(mttos), "mantenimientos")

# COMMAND ----------

# DBTITLE 1,Generador de telemetría diaria (Spark nativo)
def hrand(*cols, salt="a"):
    """Pseudo-aleatorio determinístico en [0,1) por fila (idempotente entre corridas)."""
    return (F.abs(F.xxhash64(F.lit(salt), *cols)) % F.lit(1_000_000)) / F.lit(1_000_000.0)

# Parámetros por producto: (base_fill, tendencia anual, rotación diaria)
prod_params = spark.createDataFrame([
 ("CRU-MAYA",0.62,-0.020,0.045),("CRU-ISTMO",0.58,-0.025,0.045),("CRU-OLMECA",0.55,-0.015,0.040),
 ("GAS-REG",0.55,0.025,0.070),("GAS-PRE",0.52,0.030,0.060),("DIE",0.57,0.020,0.065),
 ("TURB",0.50,0.015,0.050),("COMB",0.68,0.035,0.020),("GLP",0.60,0.000,0.060)],
 "id_producto string, base_fill double, tendencia double, rotacion double")
# Eventos de disrupción (p.ej. Huracán Otis en Acapulco, oct-2023)
eventos = spark.createDataFrame([
 ("TMR-ACA","2023-10-25","2023-12-10",-0.35),("TAD-CJU","2025-02-01","2025-02-28",-0.20),
 ("TMR-ROS","2024-08-01","2024-09-15",0.20)], "id_instalacion string, ev_ini string, ev_fin string, ajuste double")


def build_telemetria(fecha_ini: dt.date, fecha_fin: dt.date):
    # Se genera desde 1 día antes para que lag() calcule recibos/despachos del primer día del rango
    gen_ini = max(START, fecha_ini - dt.timedelta(days=1))
    n = (fecha_fin - gen_ini).days + 1
    dias = spark.range(n).select(F.date_add(F.lit(gen_ini), F.col("id").cast("int")).alias("fecha"))
    tq = spark.createDataFrame(tanques, tanques_schema).join(prod_params, "id_producto")
    mt_dias = (spark.createDataFrame(mttos, mttos_schema)
               .select("id_tanque", F.explode(F.sequence(F.to_date("fecha_inicio"), F.to_date("fecha_fin"))).alias("fecha"))
               .distinct().withColumn("en_mtto", F.lit(True)))
    inst_df = spark.createDataFrame(inst, inst_schema).select("id_instalacion", "region")
    ev = eventos.select("id_instalacion", F.to_date("ev_ini").alias("ev_ini"), F.to_date("ev_fin").alias("ev_fin"), "ajuste")
    df = (dias.crossJoin(F.broadcast(tq))
          .where((F.col("fecha") >= F.to_date("fecha_alta")) & (F.col("fecha_baja").isNull() | (F.col("fecha") < F.to_date("fecha_baja"))))
          .join(mt_dias, ["id_tanque", "fecha"], "left").withColumn("en_mtto", F.coalesce("en_mtto", F.lit(False)))
          .join(F.broadcast(inst_df), "id_instalacion")
          .join(F.broadcast(ev), "id_instalacion", "left")
          .withColumn("ajuste", F.when(F.col("fecha").between(F.col("ev_ini"), F.col("ev_fin")), F.col("ajuste")).otherwise(F.lit(0.0)))
          .drop("ev_ini", "ev_fin"))
    t_years = F.datediff("fecha", F.lit(START)) / 365.0
    phase = hrand("id_tanque", salt="ph") * 365
    per_corto = 20 + hrand("id_tanque", salt="pc") * 40
    d = F.datediff("fecha", F.lit(START))
    fill = (F.col("base_fill") + (hrand("id_tanque", salt="b") - 0.5) * 0.2
            + F.col("tendencia") * t_years
            + 0.08 * F.sin(2 * math.pi * (F.dayofyear("fecha") + phase) / 365)
            + 0.07 * F.sin(2 * math.pi * d / per_corto + phase)
            + (hrand("id_tanque", "fecha", salt="n") - 0.5) * 0.06
            + F.col("ajuste"))
    fondaje_pct = F.col("fondaje_bbl") / F.col("capacidad_nominal_bbl")
    fill = F.when(F.col("en_mtto"), fondaje_pct * 0.4).otherwise(F.least(F.lit(0.97), F.greatest(fondaje_pct + 0.02, fill)))
    df = df.withColumn("volumen_bbl", F.round(fill * F.col("capacidad_nominal_bbl"), 1))
    w = Window.partitionBy("id_tanque").orderBy("fecha")
    delta = F.col("volumen_bbl") - F.coalesce(F.lag("volumen_bbl").over(w), F.col("volumen_bbl"))
    desp_base = F.col("capacidad_nominal_bbl") * F.col("rotacion") * (0.8 + 0.4 * hrand("id_tanque", "fecha", salt="d"))
    df = (df.withColumn("delta", delta).withColumn("desp_base", F.when(F.col("en_mtto"), F.lit(0.0)).otherwise(desp_base))
            .withColumn("recibos_bbl", F.round(F.greatest(F.col("delta") + F.col("desp_base"), F.lit(0.0)), 1))
            .withColumn("despachos_bbl", F.round(F.col("recibos_bbl") - F.col("delta"), 1)))
    temp_base = (F.when(F.col("region").isin("SURESTE", "GOLFO"), 30).when(F.col("region") == "PACIFICO", 28)
                  .when(F.col("region") == "NORTE", 24).otherwise(20))
    df = df.withColumn("temperatura_c", F.round(temp_base + 6 * F.sin(2 * math.pi * (F.dayofyear("fecha") - 100) / 365)
                                                + (hrand("id_tanque", "fecha", salt="t") - 0.5) * 4, 1))
    # Ruido de calidad de datos (para expectativas del pipeline)
    q = hrand("id_tanque", "fecha", salt="q")
    df = df.withColumn("volumen_bbl", F.when(q < 0.001, F.lit(None).cast("double"))
                                       .when(q > 0.9995, F.round(F.col("capacidad_nominal_bbl") * 1.35, 1))
                                       .otherwise(F.col("volumen_bbl")))
    out = (df.where(F.col("fecha") >= F.lit(fecha_ini))
             .select(F.date_format("fecha", "yyyy-MM-dd").alias("fecha_lectura"), "id_tanque", "volumen_bbl", "recibos_bbl",
                     "despachos_bbl", "temperatura_c", F.lit("SCADA").alias("fuente"), F.current_timestamp().alias("ts_registro")))
    dups = out.where(hrand("id_tanque", "fecha_lectura", salt="dup") < 0.001)
    return out.unionByName(dups)

# COMMAND ----------

# DBTITLE 1,Escritura en landing
def write_catalogos():
    for name, rows, schema in [("productos", productos, productos_schema), ("instalaciones", inst, inst_schema),
                               ("tanques", tanques, tanques_schema), ("mantenimientos", mttos, mttos_schema)]:
        (spark.createDataFrame(rows, schema).coalesce(1).write.mode("overwrite")
         .option("header", True).csv(f"{LANDING}/catalogos/{name}"))


def write_telemetria(df, etiqueta: str):
    # Cada carga va a su propia carpeta; reprocesar la misma etiqueta la sobrescribe
    df.repartition(24 if MODO == "backfill" else 1).write.mode("overwrite").json(f"{LANDING}/telemetria_inventario/carga={etiqueta}")


write_catalogos()
etiqueta = f"{MODO}_{FECHA_INI:%Y%m%d}_{FECHA_FIN:%Y%m%d}"
write_telemetria(build_telemetria(FECHA_INI, FECHA_FIN), etiqueta)
print("OK ->", etiqueta)

# COMMAND ----------

# DBTITLE 1,Validación
# MAGIC %sql
# MAGIC -- Fill % por tipo de instalación / producto / año (esperado: 2% = mantenimiento, 135% = picos intencionales)
# MAGIC WITH t AS (SELECT * FROM read_files('/Volumes/<CATALOG>/pemex_alm_bronze/landing/telemetria_inventario/', format=>'json')),
# MAGIC tq AS (SELECT * FROM read_files('/Volumes/<CATALOG>/pemex_alm_bronze/landing/catalogos/tanques/', format=>'csv', header=>true)),
# MAGIC i AS (SELECT * FROM read_files('/Volumes/<CATALOG>/pemex_alm_bronze/landing/catalogos/instalaciones/', format=>'csv', header=>true))
# MAGIC SELECT i.tipo_instalacion, tq.id_producto, year(t.fecha_lectura) anio,
# MAGIC        count(*) filas, count(DISTINCT t.id_tanque) tanques,
# MAGIC        round(avg(t.volumen_bbl / tq.capacidad_nominal_bbl)*100,1) fill_pct_prom,
# MAGIC        sum(CASE WHEN t.volumen_bbl IS NULL THEN 1 ELSE 0 END) nulos
# MAGIC FROM t JOIN tq USING (id_tanque) JOIN i USING (id_instalacion)
# MAGIC GROUP BY ALL ORDER BY 1,2,3

# COMMAND ----------

