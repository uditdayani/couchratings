# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Step 2 – build Silver
# MAGIC
# MAGIC Parses Bronze files that Silver hasn't processed yet into clean tables:
# MAGIC
# MAGIC | Table | One row per |
# MAGIC | --- | --- |
# MAGIC | `silver.matches` | match |
# MAGIC | `silver.innings` | innings, with totals from the deliveries |
# MAGIC | `silver.deliveries` | ball, including wides and no-balls |
# MAGIC | `silver.match_players` | player in a match |
# MAGIC | `silver.players` | player (Cricsheet Register ID) |
# MAGIC
# MAGIC Files that can't be used (bad JSON, not T20) are quarantined in `ops.ingest_log` with a reason.
# MAGIC The first run processes ~4,900 matches and can take a few minutes; later runs only touch new files.

# COMMAND ----------

import os, sys, uuid
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import silver
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
t = Tables(catalog=dbutils.widgets.get("catalog"))
run_id = str(uuid.uuid4())

counts = silver.build_silver(spark, t, run_id)
print(counts)

# COMMAND ----------

# MAGIC %md ## What landed

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'matches' AS table_name, count(*) AS row_count FROM {t.silver_matches}
    UNION ALL SELECT 'innings', count(*) FROM {t.silver_innings}
    UNION ALL SELECT 'deliveries', count(*) FROM {t.silver_deliveries}
    UNION ALL SELECT 'match_players', count(*) FROM {t.silver_match_players}
    UNION ALL SELECT 'players', count(*) FROM {t.silver_players}"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT source, result, count(*) AS matches
    FROM {t.silver_matches} GROUP BY source, result ORDER BY source, matches DESC"""))

# COMMAND ----------

# MAGIC %md ## Quarantined files (expect very few, if any)

# COMMAND ----------

display(spark.sql(f"""
    SELECT match_id, detail AS reason, logged_at FROM {t.ingest_log}
    WHERE stage = 'silver' AND status = 'quarantined' ORDER BY logged_at DESC"""))

# COMMAND ----------

# MAGIC %md ## Sanity look: the highest first-innings totals

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.match_date, m.event_name, i.batting_team, i.total_runs, i.wickets, i.legal_balls
    FROM {t.silver_innings} i JOIN {t.silver_matches} m USING (match_id)
    WHERE i.innings_no = 1 ORDER BY i.total_runs DESC LIMIT 10"""))
