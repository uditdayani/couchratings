# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Step 3 – Silver data quality checks
# MAGIC
# MAGIC Runs every check on the Silver tables, saves the results to `ops.dq_results`,
# MAGIC and **fails the notebook if any error-level check fails**. In the scheduled job
# MAGIC (week 8) that stops Gold from rebuilding on bad data.
# MAGIC
# MAGIC | Check | Level |
# MAGIC | --- | --- |
# MAGIC | No duplicate matches or deliveries | error |
# MAGIC | Only T20 matches; every delivery belongs to a match | error |
# MAGIC | runs_total = batter runs + extras, and extras add up by type | error |
# MAGIC | Every batter, non-striker and bowler has a Register ID | error |
# MAGIC | 2nd-innings target = 1st-innings runs + 1 (clean matches, ≤1% may differ) | error |
# MAGIC | Legal balls within the overs limit | warning |

# COMMAND ----------

import os, sys, uuid
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import quality
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
t = Tables(catalog=dbutils.widgets.get("catalog"))

results = quality.run_checks(spark, t)
display(spark.createDataFrame(results))

# COMMAND ----------

quality.save_and_enforce(spark, t, results, run_id=str(uuid.uuid4()))
print("All error-level checks passed. Silver is ready for Gold.")
