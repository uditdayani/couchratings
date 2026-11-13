# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 – 0–10 ratings and form (Gold)
# MAGIC
# MAGIC Turns each player's match impact (in runs, from `06_gold_ratings`) into a **0–10 rating** by
# MAGIC ranking it against all rated performances **in the same competition**, then mapping the
# MAGIC percentile onto fixed anchors:
# MAGIC
# MAGIC | Percentile | 1st | 25th | 50th | 90th | 99th | best ever |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | Rating | 3.0 | 5.8 | 6.5 | 8.0 | 9.3 | 10.0 |
# MAGIC
# MAGIC Players are rated only if they faced 5 balls, bowled an over, were dismissed, or took part in a
# MAGIC dismissal. **Form** = weighted average of the last 10 ratings, halving every 5 matches back,
# MAGIC overall ("All") and per competition. Writes `gold.player_match_ratings` and `gold.player_form`.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import gold_scale
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v2")
t = Tables(catalog=dbutils.widgets.get("catalog"))
VERSION = dbutils.widgets.get("model_version")

summary = gold_scale.run_scale(spark, t, VERSION)
summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 – the scale behaves the same in every competition
# MAGIC Median about 6.5, about 1% at 9.3 or higher, and only a few below 4.

# COMMAND ----------

dist = spark.sql(f"""
    SELECT competition, count(*) AS rated,
           round(percentile_approx(rating, 0.5), 1) AS median_rating,
           round(100 * avg(CASE WHEN rating >= 9.3 THEN 1.0 ELSE 0 END), 2) AS pct_9_3_plus,
           round(100 * avg(CASE WHEN rating < 4 THEN 1.0 ELSE 0 END), 2) AS pct_below_4
    FROM {t.gold_player_match_ratings}
    WHERE version = '{VERSION}' AND involved
    GROUP BY competition ORDER BY rated DESC""")
display(dist)
for r in dist.collect():
    assert 6.2 <= r.median_rating <= 6.8, f"{r.competition}: median rating {r.median_rating} is off-scale"
print("Scale check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2 – batters and bowlers both reach the top
# MAGIC Of the 9+ ratings, what share came mainly from bowling? Week 6 tunes the wicket splits if one role dominates.

# COMMAND ----------

display(spark.sql(f"""
    SELECT competition, count(*) AS ratings_9_plus,
           round(100 * avg(CASE WHEN bowling > batting THEN 1.0 ELSE 0 END), 1) AS pct_mainly_bowling
    FROM {t.gold_player_match_ratings}
    WHERE version = '{VERSION}' AND rating >= 9
    GROUP BY competition ORDER BY ratings_9_plus DESC"""))

# COMMAND ----------

# MAGIC %md ## Best-rated performances in the latest IPL season

# COMMAND ----------

display(spark.sql(f"""
    SELECT r.match_date, p.name, r.team, r.rating, r.total_impact,
           r.runs, r.balls_faced, r.wickets, r.runs_conceded, r.balls_bowled
    FROM {t.gold_player_match_ratings} r
    JOIN {t.silver_players} p USING (player_id)
    WHERE r.version = '{VERSION}' AND r.competition = 'Indian Premier League'
      AND year(r.match_date) = (SELECT max(year(match_date)) FROM {t.gold_player_match_ratings}
                                WHERE version = '{VERSION}' AND competition = 'Indian Premier League')
    ORDER BY r.rating DESC, r.total_impact DESC LIMIT 15"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Form leaderboards
# MAGIC Active players (played in the last 12 months) with at least 5 rated matches.

# COMMAND ----------

display(spark.sql(f"""
    SELECT scope, leaderboard_rank AS rank, name, form_rating, matches_counted, last_match_date
    FROM {t.gold_player_form}
    WHERE version = '{VERSION}' AND leaderboard_rank <= 15
      AND scope IN ('Indian Premier League', 'T20I full members')
    ORDER BY scope, leaderboard_rank"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT leaderboard_rank AS rank, name, form_rating, matches_counted, last_match_date
    FROM {t.gold_player_form}
    WHERE version = '{VERSION}' AND scope = 'All' AND leaderboard_rank <= 20
    ORDER BY leaderboard_rank"""))
