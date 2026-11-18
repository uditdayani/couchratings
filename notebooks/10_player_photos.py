# Databricks notebook source
# MAGIC %md
# MAGIC # Player photos (run monthly, or by hand)
# MAGIC
# MAGIC Finds a photo for each rated player on Wikimedia Commons and stores it with the author and
# MAGIC licence, which the dashboard shows under the photo (most Commons licences require that).
# MAGIC
# MAGIC Cricsheet player id → ESPNcricinfo id (Cricsheet's people register) → Wikidata item with that
# MAGIC id (P2697) and an image (P18) → the Commons file's thumbnail, author and licence.
# MAGIC
# MAGIC Writes `silver.player_photos`. Takes a minute or two; it isn't part of the daily job.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import bronze, photos
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
t = Tables(catalog=dbutils.widgets.get("catalog"))
REGISTER_DIR = f"/Volumes/{t.catalog}/bronze/raw_files/register"
os.makedirs(REGISTER_DIR, exist_ok=True)

people_csv = f"{REGISTER_DIR}/people.csv"
size = bronze.download(photos.PEOPLE_URL, people_csv)
print(f"people.csv: {size / 1e6:.1f} MB")

# COMMAND ----------

summary = photos.build_player_photos(spark, t, people_csv)
print(summary)

# COMMAND ----------

# MAGIC %md ## Who has a photo (most-rated players first)

# COMMAND ----------

display(spark.sql(f"""
    SELECT p.name, ph.license, ph.artist, ph.image_url
    FROM {t.silver_player_photos} ph JOIN {t.silver_players} p USING (player_id)
    ORDER BY p.name LIMIT 50"""))
