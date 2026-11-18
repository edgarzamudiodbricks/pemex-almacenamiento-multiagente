# Databricks notebook source
# DBTITLE 1,Planteamiento del problema
# MAGIC %md
# MAGIC # PEMEX Almacenamiento - Pronóstico de capacidad
# MAGIC
# MAGIC * **Fuente:** `<CATALOG>.pemex_alm_gold.serie_forecast_instalacion_producto` (serie diaria sin huecos).
# MAGIC * **Frecuencia:** diaria. **Llave de serie:** `serie_id` = `id_instalacion|id_producto` (242 series).
# MAGIC * **Objetivo:** `pct_llenado` (% de llenado sobre capacidad operativa, 0-100). La capacidad disponible se deriva: `capacidad_operativa * (1 - pct/100)`.
# MAGIC * **Horizonte:** 30 días.
# MAGIC * **Covariables conocidas por adelantado:** calendario y `frac_capacidad_operativa` (capacidad operativa / nominal), calculable a futuro con el plan de mantenimientos y la vigencia de tanques. También permite escenarios what-if.
# MAGIC * **Covariables excluidas:** volumen, recibos y despachos del mismo día (solo se observan después; serían fuga de información).
# MAGIC * **Modelo:** LightGBM global sobre todas las series (cientos de series + covariables), pronóstico directo con lags >= 30 días; 3 modelos: media (p50) y cuantiles p10/p90.
# MAGIC * **Validación:** holdout de los últimos 30 días de cada serie vs baselines naive y seasonal-naive (7 días). Métrica principal: MASE.
# MAGIC * **Salidas:** modelo en UC `pemex_alm_ml.modelo_pronostico_capacidad` (alias `champion`), tablas `pemex_alm_ml.pronostico_capacidad_diario` y `pemex_alm_ml.alertas_capacidad`.

# COMMAND ----------

# DBTITLE 1,Instalar librerías
# MAGIC %pip install -q lightgbm==4.5.0 uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuración y carga de datos
import datetime as dt

import lightgbm as lgb
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from pyspark.sql import functions as F

CATALOG = "<CATALOG>"  # parámetro: ver README
GOLD, SILVER, ML = (f"{CATALOG}.pemex_alm_gold", f"{CATALOG}.pemex_alm_silver", f"{CATALOG}.pemex_alm_ml")
MODEL_NAME = f"{ML}.modelo_pronostico_capacidad"
EXPERIMENT = "/Users/<USER_EMAIL>/pemex-almacenamiento-multiagente/03_ml/exp_pronostico_capacidad"  # parámetro: ver README
HORIZONTE = 30
SEED = 42

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

STATIC_COLS = ["id_instalacion", "tipo_instalacion", "region", "id_producto", "categoria_producto"]
serie = (
    spark.table(f"{GOLD}.serie_forecast_instalacion_producto")
    .select("serie_id", "fecha", *STATIC_COLS, "pct_llenado", "capacidad_operativa_bbl", "capacidad_nominal_bbl")
    .withColumn("frac_capacidad_operativa",
                F.when(F.col("capacidad_nominal_bbl") > 0, F.col("capacidad_operativa_bbl") / F.col("capacidad_nominal_bbl"))
                 .otherwise(F.lit(0.0)))
)
pdf = serie.toPandas()
pdf["fecha"] = pd.to_datetime(pdf["fecha"])
pdf = pdf.sort_values(["serie_id", "fecha"]).reset_index(drop=True)
FECHA_CORTE = pdf["fecha"].max()
print(pdf.shape, "| series:", pdf.serie_id.nunique(), "| corte:", FECHA_CORTE.date())

# COMMAND ----------

# DBTITLE 1,EDA: estructura, historia y tipos
# Regularidad del grid y longitud de historia por serie
chk = pdf.groupby("serie_id").agg(ini=("fecha", "min"), fin=("fecha", "max"), n=("fecha", "size"))
chk["esperado"] = (chk["fin"] - chk["ini"]).dt.days + 1
print("Series con huecos:", int((chk["n"] != chk["esperado"]).sum()))
print("Duplicados (serie, fecha):", int(pdf.duplicated(["serie_id", "fecha"]).sum()))
print("Historia (días):\n", chk["n"].describe().round(0).to_string())
print("Series con < 2 años de historia:", int((chk["n"] < 730).sum()))
print("Nulos en objetivo (capacidad operativa = 0):", int(pdf["pct_llenado"].isna().sum()))
display(pdf.describe(include="all").T)

tipos = pd.DataFrame({
    "variable": ["serie_id", "tipo_instalacion", "region", "id_producto", "categoria_producto", "lag_k / media_movil",
                 "frac_capacidad_operativa", "dia_semana, mes", "dia_anio_sin/cos"],
    "tipo": ["categórica nominal (id de serie)", "categórica nominal", "categórica nominal", "categórica nominal",
             "categórica nominal", "numérica float (lags >= 30 días del objetivo)",
             "numérica float [0,1] (conocida a futuro)", "numérica entera (calendario)", "numérica float (calendario cíclico)"],
})
display(tipos)

# COMMAND ----------

# DBTITLE 1,EDA: series y estacionalidad
muestra = ["REF-TUL|DIE", "TAD-AZC|GAS-REG", "TMC-DBO|CRU-MAYA", "TMR-ACA|DIE", "TMR-MAN|COMB", "REF-OLM|GAS-REG"]
fig, axes = plt.subplots(len(muestra), 1, figsize=(14, 2.2 * len(muestra)), sharex=True)
for ax, s in zip(axes, muestra):
    d = pdf[pdf.serie_id == s]
    ax.plot(d["fecha"], d["pct_llenado"], lw=0.8)
    ax.set_title(s, fontsize=9); ax.set_ylim(0, 100); ax.set_ylabel("% llenado")
plt.tight_layout(); plt.show()

fig, axes = plt.subplots(1, 3, figsize=(16, 3.5))
pdf.assign(mes=pdf.fecha.dt.month).groupby(["mes", "categoria_producto"])["pct_llenado"].mean().unstack().plot(ax=axes[0], title="% llenado medio por mes")
pdf.assign(dow=pdf.fecha.dt.dayofweek).groupby("dow")["pct_llenado"].mean().plot(ax=axes[1], title="% llenado medio por día de semana")
pdf.assign(anio=pdf.fecha.dt.year).groupby(["anio", "id_producto"])["pct_llenado"].mean().unstack().plot(ax=axes[2], title="Tendencia anual por producto")
axes[2].legend(fontsize=6); plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Ingeniería de variables (compartida por entrenamiento y modelo)
LAGS = [30, 31, 35, 42, 60, 90, 182, 364]     # todos >= HORIZONTE: features de filas futuras salen del histórico
VENTANAS = [7, 30]                              # medias móviles que terminan en t-30
CAT_FEATS = ["serie_id", "tipo_instalacion", "region", "id_producto", "categoria_producto"]
FEATURES = ([f"lag_{l}" for l in LAGS] + [f"media_{w}d_lag30" for w in VENTANAS]
            + ["frac_capacidad_operativa", "dia_semana", "mes", "dia_anio_sin", "dia_anio_cos"] + CAT_FEATS)


def make_features(frame: pd.DataFrame, categorias: dict) -> pd.DataFrame:
    """Lags por fecha (no por posición) para que filas futuras tomen sus lags del histórico."""
    frame = frame.sort_values(["serie_id", "fecha"]).copy()
    base = frame[["serie_id", "fecha", "pct_llenado"]]
    for lag in LAGS:
        l = base.rename(columns={"pct_llenado": f"lag_{lag}"})
        l = l.assign(fecha=l["fecha"] + pd.Timedelta(days=lag))
        frame = frame.merge(l, on=["serie_id", "fecha"], how="left")
    for w in VENTANAS:
        r = base.assign(v=base.groupby("serie_id")["pct_llenado"].transform(lambda s: s.rolling(w, min_periods=1).mean()))
        r = r[["serie_id", "fecha", "v"]].rename(columns={"v": f"media_{w}d_lag30"})
        r = r.assign(fecha=r["fecha"] + pd.Timedelta(days=30))
        frame = frame.merge(r, on=["serie_id", "fecha"], how="left")
    doy = frame["fecha"].dt.dayofyear
    frame["dia_semana"] = frame["fecha"].dt.dayofweek
    frame["mes"] = frame["fecha"].dt.month
    frame["dia_anio_sin"] = np.sin(2 * np.pi * doy / 365.25)
    frame["dia_anio_cos"] = np.cos(2 * np.pi * doy / 365.25)
    for c in CAT_FEATS:
        frame[c] = pd.Categorical(frame[c], categories=categorias[c])
    return frame


CATEGORIAS = {c: sorted(pdf[c].dropna().unique().tolist()) for c in CAT_FEATS}
feats = make_features(pdf, CATEGORIAS)

# Chequeo de fuga: correlación de cada feature numérica con el objetivo
num = [f for f in FEATURES if f not in CAT_FEATS]
display(feats[num + ["pct_llenado"]].corr()["pct_llenado"].drop("pct_llenado").round(3).sort_values().to_frame("corr_con_objetivo"))

# COMMAND ----------

# DBTITLE 1,Backtest: holdout de 30 días vs baselines
PARAMS = dict(n_estimators=600, learning_rate=0.05, num_leaves=63, min_child_samples=50,
              subsample=0.8, subsample_freq=1, colsample_bytree=0.8, random_state=SEED, verbose=-1)


def entrenar(datos: pd.DataFrame) -> dict:
    filas = datos.dropna(subset=["pct_llenado", "lag_30"])
    X, y = filas[FEATURES], filas["pct_llenado"]
    return {
        "p50": lgb.LGBMRegressor(objective="regression", **PARAMS).fit(X, y),
        "p10": lgb.LGBMRegressor(objective="quantile", alpha=0.1, **PARAMS).fit(X, y),
        "p90": lgb.LGBMRegressor(objective="quantile", alpha=0.9, **PARAMS).fit(X, y),
    }, len(filas)


corte_bt = FECHA_CORTE - pd.Timedelta(days=HORIZONTE)
train = feats[feats.fecha <= corte_bt]
test = feats[(feats.fecha > corte_bt) & feats.pct_llenado.notna()].copy()
modelos_bt, n_train = entrenar(train)
for q, m in modelos_bt.items():
    test[q] = np.clip(m.predict(test[FEATURES]), 0, 100)
test["serie_id"] = test["serie_id"].astype(str)

# Baselines: naive (último valor al corte) y seasonal naive semanal
hist = pdf[["serie_id", "fecha", "pct_llenado"]]
ult = hist[hist.fecha == corte_bt][["serie_id", "pct_llenado"]].rename(columns={"pct_llenado": "naive"})
test = test.merge(ult, on="serie_id", how="left")
k = (test["fecha"] - corte_bt).dt.days
test["fecha_ref"] = test["fecha"] - pd.to_timedelta(7 * np.ceil(k / 7), unit="D")
test = test.merge(hist.rename(columns={"fecha": "fecha_ref", "pct_llenado": "snaive7"}), on=["serie_id", "fecha_ref"], how="left")

# Escala MASE por serie: MAE del naive a 1 paso en entrenamiento
escala = (train.sort_values("fecha").groupby("serie_id", observed=True)["pct_llenado"]
          .apply(lambda s: s.diff().abs().mean()).rename("escala").reset_index())
escala["serie_id"] = escala["serie_id"].astype(str)
test = test.merge(escala, on="serie_id", how="left")


def metricas(pred: str) -> dict:
    e = test[pred] - test["pct_llenado"]
    smape = (2 * e.abs() / (test[pred].abs() + test["pct_llenado"].abs()).replace(0, np.nan)).mean() * 100
    mase = (e.abs().groupby(test["serie_id"]).mean() / test.groupby("serie_id")["escala"].first()).mean()
    return {"MASE": mase, "MAE": e.abs().mean(), "RMSE": np.sqrt((e ** 2).mean()), "sMAPE": smape}


resultados = pd.DataFrame({m: metricas(m) for m in ["p50", "naive", "snaive7"]}).T.round(3)
resultados.index = ["LightGBM (p50)", "Naive", "Seasonal naive 7d"]
cobertura = ((test["pct_llenado"] >= test["p10"]) & (test["pct_llenado"] <= test["p90"])).mean()
display(resultados)
print(f"Cobertura intervalo p10-p90 en holdout: {cobertura:.1%} (objetivo teórico 80%)")

with mlflow.start_run(run_name="backtest_holdout_30d") as run_bt:
    mlflow.log_params({**PARAMS, "horizonte": HORIZONTE, "lags": str(LAGS), "ventanas": str(VENTANAS),
                       "corte_backtest": str(corte_bt.date()), "n_train": n_train, "n_test": len(test)})
    for modelo, fila in resultados.iterrows():
        pref = modelo.split(" ")[0].lower()
        mlflow.log_metrics({f"{pref}_{k}": float(v) for k, v in fila.items()})
    mlflow.log_metric("cobertura_p10_p90", float(cobertura))
    mlflow.set_tags({"proyecto": "pemex_almacenamiento", "tipo": "backtest"})

# COMMAND ----------

# DBTITLE 1,Pronóstico vs real en holdout
fig, axes = plt.subplots(2, 3, figsize=(16, 6), sharey=True)
for ax, s in zip(axes.ravel(), muestra):
    h = pdf[(pdf.serie_id == s) & (pdf.fecha > corte_bt - pd.Timedelta(days=90))]
    t = test[test.serie_id == s]
    ax.plot(h["fecha"], h["pct_llenado"], color="black", lw=0.8, label="real")
    ax.plot(t["fecha"], t["p50"], color="tab:blue", label="p50")
    ax.fill_between(t["fecha"], t["p10"], t["p90"], color="tab:blue", alpha=0.2, label="p10-p90")
    ax.plot(t["fecha"], t["snaive7"], color="tab:orange", ls="--", lw=0.8, label="snaive7")
    ax.axvline(corte_bt, color="gray", ls=":"); ax.set_title(s, fontsize=9); ax.tick_params(axis="x", labelsize=7)
axes[0, 0].legend(fontsize=7); plt.tight_layout(); plt.show()

res = test.assign(residuo=test["pct_llenado"] - test["p50"]).groupby("fecha")["residuo"].agg(["mean", "std"])
res.plot(title="Residuo medio (real - p50) por día de holdout", figsize=(10, 3)); plt.show()

# COMMAND ----------

# DBTITLE 1,Refit con todo el histórico, log, validación aislada y registro en UC
class PronosticoCapacidad(mlflow.pyfunc.PythonModel):
    """Recibe filas futuras (serie_id, fecha, frac_capacidad_operativa) y devuelve p10/p50/p90 de % llenado.

    frac_capacidad_operativa = capacidad operativa / nominal esperada ese día (1.0 = todos los tanques operativos).
    Permite escenarios what-if (p.ej. 0.8 si se planea sacar 20% de capacidad a mantenimiento).
    """

    def __init__(self, modelos, historia, estaticos, categorias, horizonte):
        self.modelos = modelos
        self.historia = historia      # últimos max(LAGS) días por serie
        self.estaticos = estaticos    # atributos por serie_id
        self.categorias = categorias
        self.horizonte = horizonte

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        fut = model_input[["serie_id", "fecha", "frac_capacidad_operativa"]].copy()
        fut["fecha"] = pd.to_datetime(fut["fecha"])
        fut["frac_capacidad_operativa"] = fut["frac_capacidad_operativa"].fillna(1.0).clip(0, 1)
        fut = fut.merge(self.estaticos, on="serie_id", how="left").assign(pct_llenado=np.nan, _futuro=True)
        hist = self.historia.assign(_futuro=False)
        # Si una fecha futura ya existe en historia, prevalece la fila futura (covariable del escenario)
        hist = hist.merge(fut[["serie_id", "fecha"]], on=["serie_id", "fecha"], how="left", indicator=True)
        hist = hist[hist["_merge"] == "left_only"].drop(columns="_merge")
        frame = make_features(pd.concat([hist, fut], ignore_index=True), self.categorias)
        rows = frame[frame["_futuro"]].copy()
        for q, m in self.modelos.items():
            rows[f"pct_llenado_{q}"] = np.clip(m.predict(rows[FEATURES]), 0, 100)
        rows["pct_llenado_p10"] = np.minimum(rows["pct_llenado_p10"], rows["pct_llenado_p50"])
        rows["pct_llenado_p90"] = np.maximum(rows["pct_llenado_p90"], rows["pct_llenado_p50"])
        rows["serie_id"] = rows["serie_id"].astype(str)
        fin_hist = self.historia.groupby("serie_id")["fecha"].max().rename("_fin").reset_index()
        fin_hist["serie_id"] = fin_hist["serie_id"].astype(str)
        rows = rows.merge(fin_hist, on="serie_id", how="left")
        rows["fuera_de_horizonte"] = ((rows["fecha"] - rows["_fin"]).dt.days > self.horizonte) | rows["_fin"].isna()
        out = model_input[["serie_id", "fecha"]].copy()
        out["fecha"] = pd.to_datetime(out["fecha"])
        return out.merge(rows[["serie_id", "fecha", "pct_llenado_p10", "pct_llenado_p50", "pct_llenado_p90", "fuera_de_horizonte"]],
                         on=["serie_id", "fecha"], how="left")


# Refit con todo el histórico (train + holdout)
modelos_final, n_final = entrenar(feats)
historia = (pdf.groupby("serie_id").tail(max(LAGS))
            [["serie_id", "fecha", "pct_llenado", "frac_capacidad_operativa", *STATIC_COLS]].reset_index(drop=True))
estaticos = pdf.drop_duplicates("serie_id")[["serie_id", *STATIC_COLS]].reset_index(drop=True)
# toPandas() de Spark Connect deja métricas pyspark en .attrs; se limpian para no serializar pyspark en el modelo
historia.attrs, estaticos.attrs = {}, {}
modelo = PronosticoCapacidad(modelos_final, historia, estaticos, CATEGORIAS, HORIZONTE)

# Filas futuras de ejemplo (frac=1.0); las reales se construyen en la celda de scoring
futuro_ej = pd.MultiIndex.from_product(
    [estaticos["serie_id"], pd.date_range(FECHA_CORTE + pd.Timedelta(days=1), periods=HORIZONTE)],
    names=["serie_id", "fecha"]).to_frame(index=False).assign(frac_capacidad_operativa=1.0)

with mlflow.start_run(run_name="modelo_final_refit") as run_final:
    mlflow.log_params({**PARAMS, "horizonte": HORIZONTE, "lags": str(LAGS), "fecha_corte": str(FECHA_CORTE.date()),
                       "n_train": n_train, "n_test": len(test), "n_final_fit": n_final})
    for modelo_n, fila in resultados.iterrows():
        pref = modelo_n.split(" ")[0].lower()
        mlflow.log_metrics({f"holdout_{pref}_{k}": float(v) for k, v in fila.items()})
    mlflow.log_metric("holdout_cobertura_p10_p90", float(cobertura))
    mlflow.set_tags({"proyecto": "pemex_almacenamiento", "tipo": "modelo_final"})
    # Requisitos explícitos: solo lo que usa el wrapper (sin pyspark/matplotlib inferidos del notebook)
    import cloudpickle
    import sklearn
    REQUISITOS = [
        f"mlflow=={mlflow.__version__}", f"cloudpickle=={cloudpickle.__version__}",
        f"lightgbm=={lgb.__version__}", f"scikit-learn=={sklearn.__version__}",
        f"pandas=={pd.__version__}", f"numpy=={np.__version__}",
    ]
    model_info = mlflow.pyfunc.log_model(
        name="model",
        python_model=modelo,
        signature=infer_signature(futuro_ej.head(100), modelo.predict(None, futuro_ej.head(100))),
        input_example=futuro_ej.head(5),
        pip_requirements=REQUISITOS,
    )

# Gate de registro: el artefacto debe cargar y predecir en un entorno limpio (como el contenedor de serving)
art = mlflow.models.Model.load(model_info.model_uri)
assert art.signature is not None, "artefacto sin signature"
ejemplo = art.load_input_example(mlflow.artifacts.download_artifacts(model_info.model_uri))
assert ejemplo is not None, "artefacto sin input example"
reqs = open(mlflow.pyfunc.get_model_dependencies(model_info.model_uri)).read()
assert "pyspark" not in reqs, reqs
print("Requisitos del modelo:\n" + reqs)
mlflow.models.predict(model_uri=model_info.model_uri, input_data=ejemplo, env_manager="uv")

version = mlflow.register_model(model_info.model_uri, MODEL_NAME, await_registration_for=300).version
mlflow.MlflowClient(registry_uri="databricks-uc").set_registered_model_alias(MODEL_NAME, "champion", version)
print("Registrado:", MODEL_NAME, "v", version, "| uri:", model_info.model_uri)

# Verificación con el modelo cargado desde UC (no con el objeto en memoria)
chk_pred = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion").predict(futuro_ej)
assert len(chk_pred) == len(futuro_ej) and chk_pred["pct_llenado_p50"].notna().all(), "el modelo no pronostica filas futuras"
display(chk_pred.head(10))

# COMMAND ----------

# DBTITLE 1,Pronóstico de 30 días con plan de mantenimientos y tablas de salida
# frac_capacidad_operativa futura por serie y día: vigencia de tanques + mantenimientos planeados
corte = FECHA_CORTE.date()
dias = spark.range(1, HORIZONTE + 1).select(F.date_add(F.lit(corte), F.col("id").cast("int")).alias("fecha"))
tq = spark.table(f"{SILVER}.dim_tanque")
mt = spark.table(f"{SILVER}.mantenimientos")
tq_dia = (dias.crossJoin(tq)
          .where((F.col("fecha") >= F.col("fecha_alta")) & (F.col("fecha_baja").isNull() | (F.col("fecha") < F.col("fecha_baja")))))
en_mt = (tq_dia.alias("t").join(mt.alias("m"), (F.col("t.id_tanque") == F.col("m.id_tanque"))
                                & F.col("t.fecha").between(F.col("m.fecha_inicio"), F.col("m.fecha_fin")), "left_semi")
         .select("id_tanque", "fecha").withColumn("en_mtto", F.lit(True)))
cov = (tq_dia.join(en_mt, ["id_tanque", "fecha"], "left")
       .groupBy(F.concat_ws("|", "id_instalacion", "id_producto").alias("serie_id"), "fecha")
       .agg(F.sum("capacidad_nominal_bbl").alias("capacidad_nominal_bbl"),
            F.sum(F.when(F.col("en_mtto").isNull(), F.col("capacidad_nominal_bbl")).otherwise(0.0)).alias("capacidad_operativa_bbl"))
       .withColumn("frac_capacidad_operativa", F.col("capacidad_operativa_bbl") / F.col("capacidad_nominal_bbl")))
futuro = cov.toPandas()
futuro["fecha"] = pd.to_datetime(futuro["fecha"])
futuro = futuro[futuro.serie_id.isin(estaticos.serie_id)]

modelo_uc = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
pred = modelo_uc.predict(futuro[["serie_id", "fecha", "frac_capacidad_operativa"]])
pred = pred.merge(futuro, on=["serie_id", "fecha"]).merge(estaticos.astype({"serie_id": str}), on="serie_id")
nombres = spark.table(f"{GOLD}.snapshot_instalacion_actual").select(
    F.concat_ws("|", "id_instalacion", "id_producto").alias("serie_id"), "nombre_instalacion", "nombre_producto",
    F.col("pct_llenado").alias("pct_llenado_actual")).toPandas()
pred = pred.merge(nombres, on="serie_id", how="left")
# Más llenado => menos espacio libre: el p10 de capacidad disponible corresponde al p90 de llenado
pred["capacidad_disponible_p50_bbl"] = (pred["capacidad_operativa_bbl"] * (1 - pred["pct_llenado_p50"] / 100)).round(1)
pred["capacidad_disponible_p10_bbl"] = (pred["capacidad_operativa_bbl"] * (1 - pred["pct_llenado_p90"] / 100)).round(1)
pred["capacidad_disponible_p90_bbl"] = (pred["capacidad_operativa_bbl"] * (1 - pred["pct_llenado_p10"] / 100)).round(1)
pred["horizonte_dias"] = (pred["fecha"] - FECHA_CORTE).dt.days
pred["fecha_corte"] = FECHA_CORTE
pred["version_modelo"] = str(version)
pred[["pct_llenado_p10", "pct_llenado_p50", "pct_llenado_p90"]] = pred[["pct_llenado_p10", "pct_llenado_p50", "pct_llenado_p90"]].round(2)
COLS = ["fecha_corte", "fecha", "horizonte_dias", "serie_id", "id_instalacion", "nombre_instalacion", "tipo_instalacion",
        "region", "id_producto", "nombre_producto", "categoria_producto", "pct_llenado_actual",
        "pct_llenado_p10", "pct_llenado_p50", "pct_llenado_p90", "capacidad_nominal_bbl", "capacidad_operativa_bbl",
        "frac_capacidad_operativa", "capacidad_disponible_p10_bbl", "capacidad_disponible_p50_bbl",
        "capacidad_disponible_p90_bbl", "fuera_de_horizonte", "version_modelo"]
sdf = (spark.createDataFrame(pred[COLS])
       .withColumn("fecha_corte", F.to_date("fecha_corte")).withColumn("fecha", F.to_date("fecha"))
       .withColumn("ts_generado", F.current_timestamp()))
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.pronostico_capacidad_diario")

# Alertas: primer día pronosticado (p50) en saturación (>90%) o desabasto (<15%) dentro del horizonte
spark.sql(f"""
CREATE OR REPLACE TABLE {ML}.alertas_capacidad
COMMENT 'Pronóstico 30 días por instalación-producto: riesgo de saturación (>90% llenado) o desabasto (<15%). Una fila por serie.'
AS
SELECT fecha_corte, serie_id, id_instalacion, nombre_instalacion, tipo_instalacion, region, id_producto, nombre_producto,
       categoria_producto,
       first(pct_llenado_actual) AS pct_llenado_actual,
       round(min(pct_llenado_p50), 2) AS pct_llenado_min_pronostico,
       round(max(pct_llenado_p50), 2) AS pct_llenado_max_pronostico,
       min(CASE WHEN pct_llenado_p50 > 90 THEN fecha END) AS fecha_primera_saturacion,
       min(CASE WHEN pct_llenado_p50 < 15 THEN fecha END) AS fecha_primer_desabasto,
       min(CASE WHEN pct_llenado_p90 > 90 THEN fecha END) AS fecha_posible_saturacion_p90,
       min(CASE WHEN pct_llenado_p10 < 15 THEN fecha END) AS fecha_posible_desabasto_p10,
       round(min(capacidad_disponible_p50_bbl), 1) AS capacidad_disponible_min_bbl,
       CASE WHEN min(CASE WHEN pct_llenado_p50 > 90 THEN fecha END) IS NOT NULL THEN 'SATURACION'
            WHEN min(CASE WHEN pct_llenado_p50 < 15 THEN fecha END) IS NOT NULL THEN 'DESABASTO'
            WHEN min(CASE WHEN pct_llenado_p90 > 90 THEN fecha END) IS NOT NULL THEN 'RIESGO_SATURACION'
            WHEN min(CASE WHEN pct_llenado_p10 < 15 THEN fecha END) IS NOT NULL THEN 'RIESGO_DESABASTO'
            ELSE 'NORMAL' END AS nivel_alerta,
       first(version_modelo) AS version_modelo
FROM {ML}.pronostico_capacidad_diario
GROUP BY ALL
""")

comentarios = {
    "fecha_corte": "Última fecha con datos reales usada para pronosticar.",
    "fecha": "Fecha pronosticada.",
    "horizonte_dias": "Días después de la fecha de corte (1-30).",
    "serie_id": "id_instalacion|id_producto.",
    "pct_llenado_actual": "% de llenado real en la fecha de corte.",
    "pct_llenado_p10": "Pronóstico de % de llenado, cuantil 10 (escenario bajo).",
    "pct_llenado_p50": "Pronóstico central de % de llenado (0-100).",
    "pct_llenado_p90": "Pronóstico de % de llenado, cuantil 90 (escenario alto).",
    "capacidad_nominal_bbl": "Capacidad nominal de tanques vigentes ese día (bbl).",
    "capacidad_operativa_bbl": "Capacidad operativa esperada según plan de mantenimientos (bbl).",
    "frac_capacidad_operativa": "Capacidad operativa / nominal esperada (1.0 = sin tanques en mantenimiento).",
    "capacidad_disponible_p10_bbl": "Espacio libre pronosticado, escenario bajo (bbl).",
    "capacidad_disponible_p50_bbl": "Espacio libre pronosticado, escenario central (bbl).",
    "capacidad_disponible_p90_bbl": "Espacio libre pronosticado, escenario alto (bbl).",
    "fuera_de_horizonte": "True si la fecha excede el horizonte entrenado (30 días).",
    "version_modelo": "Versión del modelo en UC usada.",
}
spark.sql(f"COMMENT ON TABLE {ML}.pronostico_capacidad_diario IS 'Pronóstico diario a 30 días de % de llenado (p10/p50/p90) y capacidad disponible por instalación y producto. Generado por el modelo {MODEL_NAME}.'")
for c, d in comentarios.items():
    spark.sql(f"ALTER TABLE {ML}.pronostico_capacidad_diario ALTER COLUMN {c} COMMENT '{d}'")
print("Filas pronóstico:", sdf.count())

# COMMAND ----------

# DBTITLE 1,Validación de salidas
# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS filas,
# MAGIC   count(DISTINCT serie_id) AS series,
# MAGIC   min(horizonte_dias) AS h_min, max(horizonte_dias) AS h_max,
# MAGIC   count_if(pct_llenado_p50 IS NULL) AS p50_nulos,
# MAGIC   count_if(pct_llenado_p50 < 0 OR pct_llenado_p90 > 100) AS fuera_rango,
# MAGIC   count_if(NOT (pct_llenado_p10 <= pct_llenado_p50 AND pct_llenado_p50 <= pct_llenado_p90)) AS cuantiles_desordenados,
# MAGIC   count_if(fuera_de_horizonte) AS fuera_horizonte,
# MAGIC   (SELECT map_from_entries(collect_list(struct(nivel_alerta, n))) FROM (SELECT nivel_alerta, count(*) n FROM <CATALOG>.pemex_alm_ml.alertas_capacidad GROUP BY 1)) AS alertas
# MAGIC FROM <CATALOG>.pemex_alm_ml.pronostico_capacidad_diario

# COMMAND ----------

