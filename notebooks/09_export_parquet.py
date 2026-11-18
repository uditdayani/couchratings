# Databricks notebook source
# MAGIC %md
# MAGIC # Export the dashboard's data as Parquet (daily, after validation)
# MAGIC
# MAGIC Runs the dashboard's own queries (`app/queries.py`), writes one Parquet file per dataset,
# MAGIC and publishes them to the GitHub release `data-latest`. The Streamlit app then reads those
# MAGIC public files: no SQL warehouse to wake up and no Databricks token in the app.
# MAGIC
# MAGIC | Widget | Default | |
# MAGIC | --- | --- | --- |
# MAGIC | `model_version` | `v3` | the ratings the dashboard shows |
# MAGIC | `repo` | `uditdayani/slogonomics` | where the release lives |
# MAGIC | `publish` | `yes` | `no`: only write the files to the volume (to check them) |
# MAGIC
# MAGIC **One-time setup:** a GitHub token that can write releases, stored as a Databricks secret.
# MAGIC 1. GitHub → Settings → Developer settings → Fine-grained tokens → Generate. Repository
# MAGIC    access: only `slogonomics`; permissions: **Contents: Read and write**. Note its expiry date.
# MAGIC 2. Run the next cell once with the token pasted in, then **delete the token from the cell**.

# COMMAND ----------

# One-time: store the GitHub token as a secret (then clear the token from this cell).
GITHUB_TOKEN = ""   # paste once, run, then delete
if GITHUB_TOKEN:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    if "slogonomics" not in [s.name for s in w.secrets.list_scopes()]:
        w.secrets.create_scope("slogonomics")
    w.secrets.put_secret("slogonomics", "github_token", string_value=GITHUB_TOKEN)
    print("Saved. Now delete the token from this cell.")

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))
sys.path.insert(0, os.path.abspath("../app"))   # the dashboard's queries

import queries
from cricket_ratings import export

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v3")
dbutils.widgets.text("repo", "uditdayani/slogonomics")
dbutils.widgets.dropdown("publish", "yes", ["yes", "no"])
CATALOG, VERSION = dbutils.widgets.get("catalog"), dbutils.widgets.get("model_version")
REPO = dbutils.widgets.get("repo")
OUT = f"/Volumes/{CATALOG}/bronze/raw_files/exports/{VERSION}"

written = export.export_datasets(spark, queries, CATALOG, VERSION, OUT)
for name, n in written.items():
    print(f"{name:12s} {n:>9,} rows  {os.path.getsize(f'{OUT}/{name}.parquet') / 1e6:6.1f} MB")

# COMMAND ----------

if dbutils.widgets.get("publish") == "yes":
    token = dbutils.secrets.get("slogonomics", "github_token")
    print("Published:", export.publish(OUT, REPO, token))
    print("App secret →  [data]  url =", repr(export.download_url(REPO)))
