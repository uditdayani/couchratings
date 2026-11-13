# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 – rating engine (Gold)
# MAGIC
# MAGIC Values every ball by how much it changed the batting side's expected runs, then credits players:
# MAGIC
# MAGIC | Who | Gets |
# MAGIC | --- | --- |
# MAGIC | Striker | runs off the bat + the cost of using up the ball |
# MAGIC | Batter who is out | the wicket's cost (the drop in expected runs) |
# MAGIC | Bowler | minus runs off the bat, wides and no-balls, minus the ball's cost, plus their share of wickets |
# MAGIC | Fielders | their share of wickets: caught 20%, stumped 30%, run out 100% (bowler gets the rest) |
# MAGIC
# MAGIC Expected runs = RE(balls left, wickets) × environment factor × venue factor, from the model
# MAGIC version below (run `05_gold_model` first). Writes `gold.ball_values` and `gold.player_match_impact`.
# MAGIC Impacts are in **runs**; week 5 turns them into 0–10 ratings.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../src"))

from cricket_ratings import gold_ratings
from cricket_ratings.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("model_version", "v2")
t = Tables(catalog=dbutils.widgets.get("catalog"))
VERSION = dbutils.widgets.get("model_version")

summary = gold_ratings.run_ratings(spark, t, VERSION)
summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 – the scaling is calibrated
# MAGIC Across a competition, first-innings runs added should average close to zero: a side scores about
# MAGIC what its competition, year and ground lead us to expect.

# COMMAND ----------

calib = spark.sql(f"""
    SELECT competition, count(*) AS innings, round(avg(ra), 2) AS avg_runs_added_per_innings
    FROM (SELECT match_id, competition, year, sum(runs_added) AS ra
          FROM {t.gold_ball_values} WHERE version = '{VERSION}' AND innings_no = 1
          GROUP BY match_id, competition, year)
    WHERE year >= 2019
    GROUP BY competition HAVING count(*) >= 50
    ORDER BY innings DESC""")
display(calib)

overall = spark.sql(f"""
    SELECT avg(ra) AS a FROM (SELECT match_id, sum(runs_added) AS ra FROM {t.gold_ball_values}
    WHERE version = '{VERSION}' AND innings_no = 1 AND year >= 2019 GROUP BY match_id)""").first()["a"]
print(f"Average first-innings runs added since 2019: {overall:.2f}")
assert abs(overall) < 5, "Expected runs look mis-scaled: first innings should average close to zero"

# COMMAND ----------

# MAGIC %md ## Check 2 – the biggest single-match impacts should look familiar

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.match_date, m.event_name, p.name, i.team, i.total_impact,
           i.batting, i.bowling, i.fielding,
           i.runs, i.balls_faced, i.wickets, i.runs_conceded, i.balls_bowled
    FROM {t.gold_player_match_impact} i
    JOIN {t.silver_players} p USING (player_id)
    JOIN {t.silver_matches} m USING (match_id)
    WHERE i.version = '{VERSION}'
    ORDER BY i.total_impact DESC LIMIT 20"""))

# COMMAND ----------

# MAGIC %md ### Best bowling performances

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.match_date, m.event_name, p.name, i.team, i.bowling, i.wickets,
           i.runs_conceded, i.balls_bowled, i.total_impact
    FROM {t.gold_player_match_impact} i
    JOIN {t.silver_players} p USING (player_id)
    JOIN {t.silver_matches} m USING (match_id)
    WHERE i.version = '{VERSION}'
    ORDER BY i.bowling DESC LIMIT 15"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3 – does the official player of the match rate highly?
# MAGIC A preview of the week 6 benchmark. The design target is "usually top 3".

# COMMAND ----------

display(spark.sql(f"""
    WITH ranked AS (
      SELECT match_id, player_id,
             rank() OVER (PARTITION BY match_id ORDER BY total_impact DESC) AS rk
      FROM {t.gold_player_match_impact} WHERE version = '{VERSION}'),
    pom AS (
      SELECT p.match_id, min(r.rk) AS rk
      FROM (SELECT match_id, explode(player_of_match_ids) AS player_id FROM {t.silver_matches}) p
      JOIN ranked r ON r.match_id = p.match_id AND r.player_id = p.player_id
      GROUP BY p.match_id)
    SELECT count(*) AS matches,
           round(100 * avg(CASE WHEN rk = 1 THEN 1.0 ELSE 0 END), 1) AS pct_top_rated,
           round(100 * avg(CASE WHEN rk <= 3 THEN 1.0 ELSE 0 END), 1) AS pct_top_3
    FROM pom"""))
