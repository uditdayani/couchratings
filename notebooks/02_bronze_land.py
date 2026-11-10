# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Step 1 – land raw files in Bronze
# MAGIC
# MAGIC Unzips the Cricsheet zips from week 1 and lands every match file in `bronze.match_files`,
# MAGIC unchanged. A file is stored once per version (file name + SHA-256 hash), so re-running
# MAGIC this notebook adds nothing new, and a file Cricsheet later corrects lands as a new version.
# MAGIC
# MAGIC Run on **serverless** compute. Safe to run as often as you like.

# COMMAND ----------

import os, sys, uuid
sys.path.insert(0, os.path.abspath("../src"))  # the repo's shared code

from cricket_ratings import bronze
from cricket_ratings.config import Tables, ensure_schemas

dbutils.widgets.text("catalog", "workspace")
t = Tables(catalog=dbutils.widgets.get("catalog"))
VOLUME = f"/Volumes/{t.catalog}/bronze/raw_files"
ZIP_DIR, JSON_DIR = f"{VOLUME}/zips", f"{VOLUME}/json"
run_id = str(uuid.uuid4())

ensure_schemas(spark, t)
bronze.ensure_tables(spark, t)
print("Run", run_id)

# COMMAND ----------

# MAGIC %md ## Unzip into one folder per source (e.g. `json/ipl_male/`)

# COMMAND ----------

for zip_name in sorted(os.listdir(ZIP_DIR)):
    if not zip_name.endswith("_json.zip"):
        continue
    source = zip_name.removesuffix("_json.zip")          # ipl_male, t20s_male
    n = bronze.extract_zip(f"{ZIP_DIR}/{zip_name}", f"{JSON_DIR}/{source}")
    print(f"{source}: {n} match files")

# COMMAND ----------

# MAGIC %md ## Land new files and versions in Bronze

# COMMAND ----------

incoming = bronze.read_raw_files(spark, f"{JSON_DIR}/*/*.json", run_id)
new_files = bronze.land(spark, t, incoming, run_id)
print(f"New files landed this run: {new_files}")

display(spark.sql(f"""
    SELECT source, count(*) AS files, count(DISTINCT match_id) AS matches, max(landed_at) AS last_landed
    FROM {t.bronze_files} GROUP BY source ORDER BY source"""))
