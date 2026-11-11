# Databricks notebook source
# MAGIC %md
# MAGIC # Week 3 – fit the model tables (Gold)
# MAGIC
# MAGIC Builds, under a model version (default `v1`):
# MAGIC
# MAGIC | Table | What it holds |
# MAGIC | --- | --- |
# MAGIC | `gold.run_expectancy` | Expected runs to the end of the innings for every (balls left, wickets down) state |
# MAGIC | `gold.venue_factors` | Each venue's scoring rate relative to all venues, pulled towards 1.0 for venues with few matches |
# MAGIC | `gold.model_versions` | When and how each version was fitted |
# MAGIC
# MAGIC The run-expectancy table is fitted on first innings of full-length, completed matches only
# MAGIC (a chase stops at the target, which would cut "runs to the end" short).
# MAGIC Re-running with the same version replaces it.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import gold_model
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v1")
dbutils.widgets.text("min_date", "")          # e.g. 2019-01-01 to fit on recent seasons only
t = Tables(catalog=dbutils.widgets.get("catalog"))
VERSION = dbutils.widgets.get("model_version")
MIN_DATE = dbutils.widgets.get("min_date") or None

summary = gold_model.fit_model(spark, t, VERSION, min_date=MIN_DATE,
                               notes="Week 3: run expectancy and venue factors")
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
# MAGIC ## Scoring has risen over time
# MAGIC If recent seasons score far more than the early ones, consider re-fitting on recent seasons only
# MAGIC with the `min_date` widget (for example `2019-01-01`) as model version `v2`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT year(c.match_date) AS year, m.team_type, count(*) AS matches,
           round(avg(i.total_runs), 1) AS avg_first_innings
    FROM completed c
    JOIN {t.silver_innings} i ON i.match_id = c.match_id AND i.innings_no = 1
    JOIN {t.silver_matches} m ON m.match_id = c.match_id
    GROUP BY 1, 2 ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md ## Highest- and lowest-scoring venues (10+ matches)

# COMMAND ----------

display(spark.sql(f"""
    (SELECT 'highest' AS which_end, venue, matches, runs_per_over, factor FROM {t.gold_venue_factors}
     WHERE version = '{VERSION}' AND matches >= 10 ORDER BY factor DESC LIMIT 10)
    UNION ALL
    (SELECT 'lowest', venue, matches, runs_per_over, factor FROM {t.gold_venue_factors}
     WHERE version = '{VERSION}' AND matches >= 10 ORDER BY factor ASC LIMIT 10)"""))
