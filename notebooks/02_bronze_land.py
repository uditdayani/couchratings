# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 – land raw files in Bronze
# MAGIC
# MAGIC Unzips Cricsheet zips and lands every match file in `bronze.match_files`, unchanged. A file is
# MAGIC stored once per version (file name + SHA-256 hash), so re-running adds nothing new, and a file
# MAGIC Cricsheet later corrects lands as a new version.
# MAGIC
# MAGIC | Widget | Backfill (by hand) | Daily job |
# MAGIC | --- | --- | --- |
# MAGIC | `download` | `none` (zips from week 1) | `recent`: fetch Cricsheet's last-7-days men's zip |
# MAGIC | `sources` | blank = every zip in the volume | `recent_male` |
# MAGIC
# MAGIC The recent zip holds every format and league; Silver keeps only IPL and men's T20Is.

# COMMAND ----------

import os, sys, uuid
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import bronze
from cricket_ratings.config import Tables, ensure_schemas

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("download", "none", ["none", "recent"])
dbutils.widgets.text("sources", "")          # e.g. recent_male; blank = all
t = Tables(catalog=dbutils.widgets.get("catalog"))
VOLUME = f"/Volumes/{t.catalog}/bronze/raw_files"
ZIP_DIR, JSON_DIR = f"{VOLUME}/zips", f"{VOLUME}/json"
SOURCES = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()]
run_id = str(uuid.uuid4())

ensure_schemas(spark, t)
bronze.ensure_tables(spark, t)
print("Run", run_id)

# COMMAND ----------

# MAGIC %md ## Download (daily runs)

# COMMAND ----------

if dbutils.widgets.get("download") == "recent":
    size = bronze.download(bronze.RECENT_URL, f"{ZIP_DIR}/{bronze.RECENT_ZIP}")
    print(f"Downloaded {bronze.RECENT_URL} ({size / 1e6:.1f} MB)")

# COMMAND ----------

# MAGIC %md ## Unzip into one folder per source (e.g. `json/ipl_male/`)

# COMMAND ----------

for zip_name in sorted(os.listdir(ZIP_DIR)):
    if not zip_name.endswith("_json.zip"):
        continue
    source = zip_name.removesuffix("_json.zip")          # ipl_male, t20s_male, recent_male
    if SOURCES and source not in SOURCES:
        continue
    n = bronze.extract_zip(f"{ZIP_DIR}/{zip_name}", f"{JSON_DIR}/{source}",
                           clear=source.startswith("recent"))
    print(f"{source}: {n} match files")

# COMMAND ----------

# MAGIC %md ## Land new files and versions in Bronze

# COMMAND ----------

globs = [f"{JSON_DIR}/{s}/*.json" for s in SOURCES] or [f"{JSON_DIR}/*/*.json"]
incoming = None
for g in globs:
    df = bronze.read_raw_files(spark, g, run_id)
    incoming = df if incoming is None else incoming.unionByName(df)
new_files = bronze.land(spark, t, incoming.dropDuplicates(["file_name", "file_hash"]), run_id)
print(f"New files landed this run: {new_files}")

display(spark.sql(f"""
    SELECT source, count(*) AS files, count(DISTINCT match_id) AS matches, max(landed_at) AS last_landed
    FROM {t.bronze_files} GROUP BY source ORDER BY source"""))
