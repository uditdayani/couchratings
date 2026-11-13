# Databricks notebook source
# MAGIC %md
# MAGIC # Week 6 – validation
# MAGIC
# MAGIC Does the rating system behave like a sensible cricket rating? Every check runs on one model
# MAGIC version, is saved to `ops.validation_results`, and **the notebook fails if any check fails**.
# MAGIC
# MAGIC | Check | Target |
# MAGIC | --- | --- |
# MAGIC | Calibration: first-innings runs added per competition, since 2019 | within ±3 runs |
# MAGIC | Median rating per competition | 6.2–6.8 |
# MAGIC | Share of ratings at 9.3+ per competition | 0.5–2% |
# MAGIC | Share of 9+ ratings driven mainly by bowling | 25–55% |
# MAGIC | Player of the match among the top 3 rated | at least 70% of matches |
# MAGIC | Famous performances rating 9+ | at least 80% of those found |

# COMMAND ----------

import os, sys, uuid
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import validation
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v2")
t = Tables(catalog=dbutils.widgets.get("catalog"))
VERSION = dbutils.widgets.get("model_version")

results, famous = validation.run_validation(spark, t, VERSION)
display(spark.createDataFrame(results, "check string, scope string, value double, target string, "
                                       "passed boolean, detail string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Famous performances
# MAGIC A blank rating means the match or player wasn't found: check the date and name pattern in
# MAGIC `validation.FAMOUS` (Cricsheet names are like "RG Sharma", dates are local match dates).

# COMMAND ----------

display(famous)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the player of the match isn't in our top 3
# MAGIC Awards are given by people, often to batters on the winning side. Comparing the misses with the
# MAGIC hits shows whether the model or the award is the outlier: worth stating in the README.

# COMMAND ----------

display(spark.sql(f"""
    WITH ranked AS (
      SELECT match_id, player_id, team, rating, batting, bowling,
             rank() OVER (PARTITION BY match_id ORDER BY rating DESC, total_impact DESC) AS rk
      FROM {t.gold_player_match_ratings} WHERE version = '{VERSION}' AND involved),
    pom AS (SELECT match_id, winner, explode(player_of_match_ids) AS player_id FROM {t.silver_matches}),
    j AS (SELECT p.match_id, p.winner, r.team, r.rating, r.rk, r.batting, r.bowling
          FROM pom p JOIN ranked r ON r.match_id = p.match_id AND r.player_id = p.player_id)
    SELECT CASE WHEN rk <= 3 THEN 'in our top 3' ELSE 'outside our top 3' END AS award_winner,
           count(*) AS matches,
           round(avg(rating), 2) AS avg_rating_of_award_winner,
           round(percentile_approx(rk, 0.5), 0) AS median_rank,
           round(100 * avg(CASE WHEN team = winner THEN 1.0 ELSE 0 END), 1) AS pct_on_winning_side,
           round(100 * avg(CASE WHEN batting > bowling THEN 1.0 ELSE 0 END), 1) AS pct_mainly_batting
    FROM j GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md ### In those misses, who did we rate highest instead?

# COMMAND ----------

display(spark.sql(f"""
    WITH ranked AS (
      SELECT match_id, player_id, team, rating, batting, bowling,
             rank() OVER (PARTITION BY match_id ORDER BY rating DESC, total_impact DESC) AS rk
      FROM {t.gold_player_match_ratings} WHERE version = '{VERSION}' AND involved),
    pom AS (SELECT match_id, winner, explode(player_of_match_ids) AS player_id FROM {t.silver_matches}),
    missed AS (SELECT p.match_id, p.winner FROM pom p
               JOIN ranked r ON r.match_id = p.match_id AND r.player_id = p.player_id
               GROUP BY p.match_id, p.winner HAVING min(r.rk) > 3)
    SELECT count(*) AS missed_matches,
           round(100 * avg(CASE WHEN r.team = m.winner THEN 1.0 ELSE 0 END), 1) AS pct_our_top_on_winning_side,
           round(100 * avg(CASE WHEN r.bowling > r.batting THEN 1.0 ELSE 0 END), 1) AS pct_our_top_mainly_bowling,
           round(avg(r.rating), 2) AS avg_rating_of_our_top
    FROM missed m JOIN ranked r ON r.match_id = m.match_id AND r.rk = 1"""))

# COMMAND ----------

# MAGIC %md ## Save results and enforce

# COMMAND ----------

validation.save_and_enforce(spark, t, results, VERSION, run_id=str(uuid.uuid4()))
print(f"All validation checks passed for model version {VERSION}.")
