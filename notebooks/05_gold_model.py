# Databricks notebook source
# MAGIC %md
# MAGIC # Week 3 – fit the model tables (Gold)
# MAGIC
# MAGIC Builds, under a model version (default `v2`):
# MAGIC
# MAGIC | Table | What it holds |
# MAGIC | --- | --- |
# MAGIC | `gold.run_expectancy` | Expected runs to the end of the innings for every (balls left, wickets down) state |
# MAGIC | `gold.environment_factors` | How high-scoring each competition was each year (IPL 2025, T20I 2024, …) |
# MAGIC | `gold.venue_factors` | How high-scoring each ground is *within its competition* |
# MAGIC | `gold.model_versions` | When and how each version was fitted |
# MAGIC
# MAGIC Expected runs in a match = RE(balls left, wickets) × environment factor × venue factor.
# MAGIC
# MAGIC The run-expectancy shape is fitted on first innings of full-length, completed matches from
# MAGIC `min_date` (default 2019) onwards; environment and venue factors cover every season, so older
# MAGIC matches can still be rated. Re-running with the same version replaces it.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import gold_model
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v2")
dbutils.widgets.text("min_date", "2019-01-01")   # fit the RE shape on modern seasons; blank = all
t = Tables(catalog=dbutils.widgets.get("catalog"))
VERSION = dbutils.widgets.get("model_version")
MIN_DATE = dbutils.widgets.get("min_date") or None

summary = gold_model.fit_model(spark, t, VERSION, min_date=MIN_DATE,
                               notes="RE shape from 2019; environment and venue factors")
summary

# COMMAND ----------

# MAGIC %md ## The run-expectancy table at a glance
# MAGIC Expected runs from that point to the end of the innings. Read across: each wicket costs runs.
# MAGIC Read down: fewer balls left, fewer runs.

# COMMAND ----------

display(spark.sql(f"""
    SELECT * FROM (
      SELECT balls_left, wickets, expected_runs FROM {t.gold_run_expectancy}
      WHERE version = '{VERSION}' AND balls_left IN (120, 96, 72, 60, 48, 36, 24, 12, 6) AND wickets < 10)
    PIVOT (first(expected_runs) FOR wickets IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    ORDER BY balls_left DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### As a chart
# MAGIC After this cell runs, click **+ → Visualization**, choose **Line**, X = `balls_left`,
# MAGIC Y = `expected_runs`, Group by = `wickets`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT balls_left, CAST(wickets AS STRING) AS wickets, expected_runs
    FROM {t.gold_run_expectancy}
    WHERE version = '{VERSION}' AND wickets IN (0, 2, 4, 6, 8)
    ORDER BY wickets, balls_left"""))

# COMMAND ----------

# MAGIC %md ## How much of the table comes straight from data

# COMMAND ----------

display(spark.sql(f"""
    SELECT method, count(*) AS cells, sum(sample_size) AS innings_states
    FROM {t.gold_run_expectancy} WHERE version = '{VERSION}'
    GROUP BY method ORDER BY cells DESC"""))

# COMMAND ----------

# MAGIC %md ## Sanity checks

# COMMAND ----------

completed = gold_model.completed_matches(spark, t, MIN_DATE)
completed.createOrReplaceTempView("completed")

phases = {r.phase: r.runs_per_over for r in spark.sql(f"""
    SELECT CASE WHEN d.over < 6 THEN 'powerplay' WHEN d.over < 15 THEN 'middle' ELSE 'death' END AS phase,
           round(sum(d.runs_total) / sum(CAST(d.is_legal AS INT)) * 6, 2) AS runs_per_over
    FROM {t.silver_deliveries} d JOIN completed c USING (match_id)
    WHERE d.innings_no = 1 AND NOT d.is_super_over
    GROUP BY 1""").collect()}
start = summary["expected_first_innings_total"]

print(f"Expected first-innings total (120 balls, 0 down): {start:.1f}")
print(f"Runs per over by phase: {phases}")
assert 120 <= start <= 200, "Expected first-innings total is outside a plausible T20 range"
assert phases["death"] > phases["middle"], "Death overs should score faster than middle overs"
print("Sanity checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring environment by competition and year
# MAGIC `factor` scales expected runs: above 1 means a higher-scoring competition-season than the
# MAGIC 2019+ baseline. Expect the IPL to climb sharply from 2023 (Impact Player rule) and T20Is to sit below 1.

# COMMAND ----------

display(spark.sql(f"""
    SELECT competition, year, matches, avg_first_innings, raw_factor, factor
    FROM {t.gold_environment_factors}
    WHERE version = '{VERSION}' AND competition IN ('Indian Premier League', 'T20I')
    ORDER BY competition, year"""))

# COMMAND ----------

# MAGIC %md ## Highest- and lowest-scoring grounds within their competition (10+ matches)

# COMMAND ----------

display(spark.sql(f"""
    (SELECT 'highest' AS which_end, venue, matches, runs_per_over, factor FROM {t.gold_venue_factors}
     WHERE version = '{VERSION}' AND matches >= 10 ORDER BY factor DESC LIMIT 10)
    UNION ALL
    (SELECT 'lowest', venue, matches, runs_per_over, factor FROM {t.gold_venue_factors}
     WHERE version = '{VERSION}' AND matches >= 10 ORDER BY factor ASC LIMIT 10)"""))
