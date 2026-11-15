"""Gold: turn match impacts (in runs) into 0-10 ratings, and ratings into form.

Rating (role-relative, V2.1): each part of a performance is ranked against the same kind
of performance in the same competition (IPL against IPL, full-member T20Is against
full-member T20Is, ...):
  - batting against batters who came in at the same stage (opener, top order, middle
    order, finisher), so a finisher's 20 off 6 competes with other finishers' innings;
  - bowling (an over or more) against other bowling performances.
Each rank becomes a score on the same scale (0 = a typical performance of that kind); a
player who batted and bowled gets the two added (V2.1.1), so an average spell leaves a
batter's score unchanged, a good one adds and a poor one subtracts, plus fielding. The
combined score is ranked within the competition and that percentile is mapped onto fixed
anchor points, interpolating between them:

    percentile  1st  25th  50th  90th  99th  best
    rating      3.0   5.8   6.5   8.0   9.3  10.0

So 9.3 means "top 1% in this competition". Players with too little involvement get no
rating (shown as "-").

Form: a weighted average of a player's last 10 ratings, each match counting half as
much every 5 matches back. Computed over all competitions ("All") and per competition.
"""
import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Tables
from .gold_ratings import rated_matches

ANCHORS = [(0.0, 3.0), (0.01, 3.0), (0.25, 5.8), (0.50, 6.5), (0.90, 8.0), (0.99, 9.3), (1.0, 10.0)]
MIN_BALLS_FACED = 5
MIN_BALLS_BOWLED = 6
FORM_MATCHES = 10
FORM_HALF_LIFE = 5
FORM_MIN_MATCHES = 3
LEADERBOARD_MIN_MATCHES = 5
ACTIVE_DAYS = 365


def involved() -> Column:
    """Faced 5 balls, bowled an over, was dismissed, or took part in a dismissal."""
    return ((F.col("balls_faced") >= MIN_BALLS_FACED) | (F.col("balls_bowled") >= MIN_BALLS_BOWLED)
            | (F.col("dismissed") > 0)
            | (F.col("wickets") + F.col("catches") + F.col("stumpings") + F.col("run_outs") > 0))


def anchor_rating(p: Column) -> Column:
    """Piecewise-linear map from a percentile (0-1) to a rating (3-10)."""
    expr = None
    for (p0, r0), (p1, r1) in zip(ANCHORS, ANCHORS[1:]):
        value = F.lit(r0) + (p - F.lit(p0)) / F.lit(p1 - p0) * F.lit(r1 - r0)
        expr = F.when(p <= p1, value) if expr is None else expr.when(p <= p1, value)
    return expr.otherwise(F.lit(ANCHORS[-1][1]))


LOGISTIC_SD = 1.8138   # pi / sqrt(3): spread of a logit score, used to put fielding runs on it


def _logit_rank(value: Column, partition: list) -> Column:
    """Rank within `partition` as a mid-rank percentile u in (0, 1), turned into a logit
    score ln(u / (1 - u)): 0 is typical, about +-2.2 the top or bottom 10%. Ties share a rank."""
    w = Window.partitionBy(*partition).orderBy(value)
    n = F.count(F.lit(1)).over(Window.partitionBy(*partition))
    u = (F.percent_rank().over(w) * (n - 1) + F.lit(0.5)) / n
    return F.log(u / (F.lit(1.0) - u))


def rate(impacts: DataFrame) -> DataFrame:
    """Add `involved`, the component scores, `score`, `percentile` (within competition) and
    `rating` (null if not involved). See the module docstring for the method.

    Runs are divided by the match's scoring level (`scale`: environment x venue factor,
    1.0 if absent) before ranking, so +40 at a par of 196 counts like +32 at a par of 160.
    Without `batting_stage`, all batters form one group; without batting/bowling/fielding
    columns, `total_impact` is treated as batting.
    """
    cols = impacts.columns
    scale = F.coalesce(F.col("scale") if "scale" in cols else F.lit(1.0), F.lit(1.0))
    bat = F.col("batting") if "batting" in cols else F.col("total_impact")
    bowl = F.col("bowling") if "bowling" in cols else F.lit(0.0)
    field = F.col("fielding") if "fielding" in cols else F.lit(0.0)
    stage = F.coalesce(F.col("batting_stage") if "batting_stage" in cols else F.lit(None), F.lit("all"))

    df = (impacts.withColumn("involved", involved())
          .withColumn("level_impact", F.round(F.col("total_impact") / scale, 3))
          .withColumn("_bat", bat / scale).withColumn("_bowl", bowl / scale).withColumn("_field", field / scale)
          .withColumn("_stage", stage)
          .withColumn("_batted", (F.col("balls_faced") > 0) | (F.col("dismissed") > 0))
          .withColumn("_bowled", F.col("balls_bowled") >= MIN_BALLS_BOWLED))
    inv = df.filter("involved")

    batted = inv.filter("_batted").select(
        "match_id", "player_id", _logit_rank(F.col("_bat"), ["competition", "_stage"]).alias("batting_score"))
    bowled = inv.filter("_bowled").select(
        "match_id", "player_id", _logit_rank(F.col("_bowl"), ["competition"]).alias("bowling_score"))
    # Runs not covered by a ranked part (fielding, and bowling under an over) go on the
    # same scale using the competition's spread of impact: about +0.3 for a typical catch.
    spread = inv.groupBy("competition").agg(F.stddev(F.col("level_impact")).alias("_spread"))

    extra = F.col("_field") + F.when(F.col("_bowled"), F.lit(0.0)).otherwise(F.col("_bowl"))
    rated = (inv.join(batted, ["match_id", "player_id"], "left")
             .join(bowled, ["match_id", "player_id"], "left")
             .join(spread, "competition", "left")
             .withColumn("score",
                         F.coalesce("batting_score", F.lit(0.0)) + F.coalesce("bowling_score", F.lit(0.0))
                         + extra / F.greatest(F.coalesce("_spread", F.lit(1.0)), F.lit(1e-6)) * F.lit(LOGISTIC_SD)))
    by_comp = Window.partitionBy("competition").orderBy("score")
    rated = (rated.withColumn("percentile", F.percent_rank().over(by_comp))
             .withColumn("rating", F.round(anchor_rating(F.col("percentile")), 1))
             .drop("_spread"))
    others = (df.filter("NOT involved")
              .withColumn("batting_score", F.lit(None).cast("double"))
              .withColumn("bowling_score", F.lit(None).cast("double"))
              .withColumn("score", F.lit(None).cast("double"))
              .withColumn("percentile", F.lit(None).cast("double"))
              .withColumn("rating", F.lit(None).cast("double")))
    out = rated.unionByName(others).drop("_bat", "_bowl", "_field", "_stage", "_batted", "_bowled")
    return (out.withColumn("percentile", F.round("percentile", 5))
               .withColumn("batting_score", F.round("batting_score", 4))
               .withColumn("bowling_score", F.round("bowling_score", 4))
               .withColumn("score", F.round("score", 4)))


def form(ratings: DataFrame, as_of) -> DataFrame:
    """Form per (scope, player): scope "All" plus each competition the player played in.

    `ratings` needs match_id, match_date, player_id, competition, rating (nulls ignored).
    Players need FORM_MIN_MATCHES rated matches. `active` = played within ACTIVE_DAYS of
    `as_of`; active players with LEADERBOARD_MIN_MATCHES get a leaderboard rank.
    """
    r = ratings.filter(F.col("rating").isNotNull()).select(
        "match_id", "match_date", "player_id", "competition", "rating")
    scoped = r.withColumn("scope", F.lit("All")).unionByName(r.withColumn("scope", F.col("competition")))

    newest_first = Window.partitionBy("scope", "player_id").orderBy(
        F.col("match_date").desc(), F.col("match_id").desc())
    recent = (scoped.withColumn("i", F.row_number().over(newest_first) - 1)
              .filter(F.col("i") < FORM_MATCHES)
              .withColumn("w", F.pow(F.lit(0.5), F.col("i") / F.lit(FORM_HALF_LIFE))))
    f = (recent.groupBy("scope", "player_id")
         .agg((F.sum(F.col("w") * F.col("rating")) / F.sum("w")).alias("form_rating"),
              F.count("*").alias("matches_counted"),
              F.max("match_date").alias("last_match_date"))
         .filter(F.col("matches_counted") >= FORM_MIN_MATCHES)
         .withColumn("form_rating", F.round("form_rating", 2))
         .withColumn("active", F.datediff(F.lit(as_of).cast("date"), F.col("last_match_date")) <= ACTIVE_DAYS))

    eligible = F.col("active") & (F.col("matches_counted") >= LEADERBOARD_MIN_MATCHES)
    by_scope = Window.partitionBy("scope", "eligible").orderBy(F.col("form_rating").desc(), F.col("player_id"))
    return (f.withColumn("eligible", eligible)
             .withColumn("leaderboard_rank", F.when(F.col("eligible"), F.row_number().over(by_scope)))
             .drop("eligible"))


def _record_params(spark: SparkSession, t: Tables, version: str) -> None:
    """Add the rating settings to this version's row in gold.model_versions."""
    rows = spark.table(t.gold_model_versions).filter(F.col("version") == version).collect()
    if not rows:
        return
    row = rows[0].asDict()
    params = json.loads(row["params"] or "{}")
    params.update({"anchors": ANCHORS, "min_balls_faced": MIN_BALLS_FACED,
                   "min_balls_bowled": MIN_BALLS_BOWLED, "form_matches": FORM_MATCHES,
                   "form_half_life": FORM_HALF_LIFE, "form_min_matches": FORM_MIN_MATCHES,
                   "rating_scope": "role-relative: batting by entry stage, bowling vs bowling, added",
                   "rating_rank_value": "total impact / match scoring level (environment x venue)"})
    row["params"] = json.dumps(params)
    schema = spark.table(t.gold_model_versions).schema
    df = spark.createDataFrame([tuple(row[f.name] for f in schema.fields)], schema)
    storage.replace_rows(spark, t.gold_model_versions, df, df.select("version"), "version")


def run_scale(spark: SparkSession, t: Tables, version: str) -> dict:
    """Ratings and form for one model version (re-running replaces it). Needs 06 first."""
    impacts = spark.table(t.gold_player_match_impact).filter(F.col("version") == version).drop("version")
    if impacts.limit(1).count() == 0:
        raise ValueError(f"No match impacts for model version {version}: run 06_gold_ratings first")

    scale = (spark.table(t.gold_ball_values).filter(F.col("version") == version)
             .groupBy("match_id").agg(F.first("scale").alias("scale")))
    context = (rated_matches(spark, t).select("match_id", "competition")
               .join(spark.table(t.silver_matches).select("match_id", "match_date"), "match_id")
               .join(scale, "match_id", "left"))
    key = spark.createDataFrame([(version,)], "version string")

    ratings = rate(impacts.join(context, "match_id")).withColumn("version", F.lit(version))
    storage.replace_rows(spark, t.gold_player_match_ratings, ratings, key, "version")

    stored = spark.table(t.gold_player_match_ratings).filter(F.col("version") == version)
    as_of = spark.table(t.silver_matches).agg(F.max("match_date")).first()[0]
    names = spark.table(t.silver_players).select("player_id", "name")
    f = form(stored, as_of).join(names, "player_id", "left").withColumn("version", F.lit(version))
    storage.replace_rows(spark, t.gold_player_form, f, key, "version")
    _record_params(spark, t, version)

    return {"version": version, "as_of": str(as_of),
            "rated_performances": stored.filter("involved").count(),
            "not_rated": stored.filter("NOT involved").count(),
            "players_with_form": spark.table(t.gold_player_form)
                                      .filter((F.col("version") == version) & (F.col("scope") == "All")).count()}
