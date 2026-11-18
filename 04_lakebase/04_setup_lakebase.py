# Databricks notebook source
# DBTITLE 1,Título
# MAGIC %md
# MAGIC # PEMEX Almacenamiento - Lakebase y App de chat multiagente
# MAGIC
# MAGIC Provisiona el proyecto Lakebase Autoscaling que guarda las conversaciones del chatbot. La App ruteará preguntas de datos al Genie Agent (`<GENIE_SPACE_ID>`) y escenarios what-if al endpoint `modelo_pronostico_capacidad`.

# COMMAND ----------

# DBTITLE 1,Actualizar databricks-sdk
import importlib.metadata as md
import subprocess
import sys

before = md.version("databricks-sdk")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--upgrade", "databricks-sdk>=0.118.0", "psycopg[binary]>=3.1"])
after = md.version("databricks-sdk")
print(f"databricks-sdk: {before} -> {after}")
if before != after:
    dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuración
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PROJECT_ID = "pemex-almacenamiento-chat"
PROJECT_NAME = f"projects/{PROJECT_ID}"
BRANCH_NAME = f"{PROJECT_NAME}/branches/production"
DATABASE = "databricks_postgres"
GENIE_SPACE_ID = "<GENIE_SPACE_ID>"  # parámetro: ver README
SERVING_ENDPOINT = "modelo_pronostico_capacidad"
print(w.current_user.me().user_name)

# COMMAND ----------

# DBTITLE 1,Crear proyecto Lakebase (idempotente)
from databricks.sdk.errors import NotFound
from databricks.sdk.service.postgres import Project, ProjectSpec

try:
    project = w.postgres.get_project(name=PROJECT_NAME)
    print("Ya existe:", project.name)
except NotFound:
    project = w.postgres.create_project(
        project=Project(spec=ProjectSpec(display_name="PEMEX Almacenamiento - Chat", pg_version=17)),
        project_id=PROJECT_ID,
    ).wait()
    print("Creado:", project.name)

# COMMAND ----------

# DBTITLE 1,Verificar branch y endpoint
for b in w.postgres.list_branches(parent=PROJECT_NAME):
    print("branch:", b.name, b.status.current_state if b.status else None)
endpoints = list(w.postgres.list_endpoints(parent=BRANCH_NAME))
for e in endpoints:
    print("endpoint:", e.name, e.status.current_state, e.status.hosts.host)
ENDPOINT_NAME = endpoints[0].name
ENDPOINT_HOST = endpoints[0].status.hosts.host

# COMMAND ----------

# DBTITLE 1,Prueba de conexión con OAuth
import psycopg

cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
with psycopg.connect(
    host=ENDPOINT_HOST,
    dbname=DATABASE,
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode="require",
    connect_timeout=30,
) as conn:
    print(conn.execute("SELECT version(), current_user").fetchone())