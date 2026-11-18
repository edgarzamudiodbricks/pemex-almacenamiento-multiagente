"""Configuracion de la app. Los valores llegan como variables de entorno desde los recursos de app.yaml."""

import os

# Valores por defecto solo para desarrollo local; en la App llegan de los recursos (app.yaml).
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "<GENIE_SPACE_ID>")
SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT", "modelo_pronostico_capacidad")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "<WAREHOUSE_ID>")

LAKEBASE_BRANCH = "projects/pemex-almacenamiento-chat/branches/production"
LAKEBASE_ENDPOINT_DEFAULT = f"{LAKEBASE_BRANCH}/endpoints/primary"
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", LAKEBASE_ENDPOINT_DEFAULT)
PGHOST = os.getenv("PGHOST", "<LAKEBASE_HOST>")
PGDATABASE = os.getenv("PGDATABASE", "databricks_postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER")  # client id del SP en la app; en local se usa el usuario actual
CHAT_SCHEMA = os.getenv("CHAT_SCHEMA", "pemex_chat")

CATALOG = os.getenv("PEMEX_CATALOG", "<CATALOG>")
TABLA_SERIES = f"{CATALOG}.pemex_alm_gold.serie_forecast_instalacion_producto"
TABLA_PRONOSTICO = f"{CATALOG}.pemex_alm_ml.pronostico_capacidad_diario"
TABLA_ALERTAS = f"{CATALOG}.pemex_alm_ml.alertas_capacidad"

HORIZONTE_MAX_DIAS = 30
MAX_FILAS_GUARDADAS = 200  # filas de resultados de Genie que se guardan en Lakebase

NOTA_MODELO = (
    "Nota: en la version actual del modelo el efecto de `frac_capacidad_operativa` es debil y "
    "puede ser contraintuitivo: una reduccion de capacidad operativa tiende a bajar ligeramente el "
    "% de llenado pronosticado, cuando fisicamente se esperaria que subiera. Ademas la respuesta es "
    "escalonada (modelo de arboles): valores cercanos, p. ej. 70% y 80%, pueden dar el mismo resultado. Interprete el resultado "
    "como sensibilidad del modelo, no como simulacion fisica del tanque."
)
