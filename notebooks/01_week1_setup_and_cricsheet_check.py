# Databricks notebook source
# MAGIC %md
# MAGIC # Week 1 – setup and Cricsheet check
# MAGIC
# MAGIC Run all cells on **serverless** compute. This notebook:
# MAGIC 1. Creates the `bronze`, `silver`, `gold` and `ops` schemas and a `raw_files` volume.
# MAGIC 2. Checks whether serverless compute can reach cricsheet.org.
# MAGIC 3. Downloads the IPL and men's T20I JSON zips into the volume (or uses zips you uploaded by hand).
# MAGIC 4. Counts the match files and peeks at one.
# MAGIC 5. Writes the results to `ops.connectivity_check`, which the Streamlit app reads.
# MAGIC
# MAGIC Data: Cricsheet (cricsheet.org), Open Data Commons Attribution License.

# COMMAND ----------

CATALOG = "workspace"  # Free Edition's default catalog
SCHEMAS = ["bronze", "silver", "gold", "ops"]
VOLUME_DIR = f"/Volumes/{CATALOG}/bronze/raw_files/zips"

ZIPS = {
    "ipl": "https://cricsheet.org/downloads/ipl_male_json.zip",
    "t20i_men": "https://cricsheet.org/downloads/t20s_male_json.zip",
}

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")

import os
os.makedirs(VOLUME_DIR, exist_ok=True)
print("Schemas and volume ready:", VOLUME_DIR)

# COMMAND ----------

# MAGIC %md ## Can serverless compute reach Cricsheet?

# COMMAND ----------

import requests

checks = []  # (check_name, passed, detail)

try:
    r = requests.head("https://cricsheet.org/downloads/", timeout=20, allow_redirects=True)
    reachable = r.status_code == 200
    detail = f"HTTP {r.status_code}"
except Exception as e:
    reachable = False
    detail = f"{type(e).__name__}: {e}"[:300]

checks.append(("cricsheet_reachable", reachable, detail))
print("Cricsheet reachable:", reachable, "-", detail)

if not reachable:
    print(
        "\nOutbound access looks blocked. Options:\n"
        "  1. Verify your Free Edition account with LinkedIn (unlocks outbound internet), then re-run.\n"
        "  2. Run scripts/download_cricsheet.py on your laptop and upload the two zips to\n"
        f"     {VOLUME_DIR} via Catalog Explorer, then re-run this notebook."
    )

# COMMAND ----------

# MAGIC %md ## Get the zips

# COMMAND ----------

import shutil

for name, url in ZIPS.items():
    target = f"{VOLUME_DIR}/{url.rsplit('/', 1)[1]}"
    if os.path.exists(target):
        print(f"{name}: already in the volume ({os.path.getsize(target) / 1e6:.1f} MB)")
        continue
    if not reachable:
        print(f"{name}: missing – upload {os.path.basename(target)} by hand")
        continue
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(target, "wb") as f:
            shutil.copyfileobj(resp.raw, f)
    print(f"{name}: downloaded ({os.path.getsize(target) / 1e6:.1f} MB)")

# COMMAND ----------

# MAGIC %md ## Count match files and peek at one

# COMMAND ----------

import json, zipfile

for name, url in ZIPS.items():
    target = f"{VOLUME_DIR}/{url.rsplit('/', 1)[1]}"
    if not os.path.exists(target):
        checks.append((f"{name}_zip", False, "zip not found in volume"))
        continue
    with zipfile.ZipFile(target) as z:
        match_files = [n for n in z.namelist() if n.endswith(".json")]
        sample = json.loads(z.read(match_files[0]))
    info = sample["info"]
    detail = (
        f"{len(match_files)} matches; sample {match_files[0]}: "
        f"{info.get('match_type')} {' v '.join(info.get('teams', []))} on {info.get('dates', ['?'])[0]}"
    )
    checks.append((f"{name}_zip", len(match_files) > 0, detail))
    print(f"{name}: {detail}")

# COMMAND ----------

# MAGIC %md ## Save results for the Streamlit check

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame(checks, "check_name string, passed boolean, detail string") \
          .withColumn("checked_at", F.current_timestamp())
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.ops.connectivity_check")
display(spark.table(f"{CATALOG}.ops.connectivity_check"))
