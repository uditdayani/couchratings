"""Gold rating engine: value every ball, then credit players.

For each ball, with expected runs E = RE(balls left, wickets) x environment x venue:

  runs added  = runs scored + E(after) - E(before)
              = runs scored + ball cost + wicket cost
  ball cost   = E(balls after, wickets before) - E(before)   (using up a ball; 0 on wides/no-balls)
  wicket cost = E(after) - E(balls after, wickets before)    (0 unless a batter is out)

Credits (all in runs):
  striker   runs off the bat + ball cost
  out batter  wicket cost (split if two dismissals on one ball)
  bowler    -(runs off the bat + wides + no-balls) - ball cost - bowler share x wicket cost
  fielders  - fielder share x wicket cost, split between the fielders named

Byes and leg-byes are not charged to the bowler, and are not the batter's runs.
Super overs are not rated. No-result matches are not rated.
"""
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Tables
from .gold_model import MAX_WICKETS, TOTAL_BALLS, competition_col
from .schema import NOT_DISMISSALS

# Share of a wicket's value earned by the bowler and by the fielders (starting values, tuned in week 6).
BOWLER_SHARE = {"bowled": 1.0, "lbw": 1.0, "hit wicket": 1.0, "caught and bowled": 1.0,
                "caught": 0.8, "stumped": 0.7}
FIELDER_SHARE = {"caught": 0.2, "stumped": 0.3, "run out": 1.0}

NUMBERS = {  # every credit row carries all of these
    "batting": "double", "bowling": "double", "fielding": "double",
    "runs": "int", "balls_faced": "int", "dismissed": "int",
    "balls_bowled": "int", "runs_conceded": "int", "wickets": "int",
    "catches": "int", "stumpings": "int", "run_outs": "int",
}


def _lookup(mapping: dict, key: Column) -> Column:
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.coalesce(F.try_element_at(m, key), F.lit(0.0))


def allotted_balls(target_overs: Column, overs_limit: Column, balls_per_over: Column) -> Column:
    """Legal balls each side could face: the reduced target overs when a match was
    shortened (cricket notation: 17.4 = 17 overs and 4 balls), otherwise the full limit."""
    full = F.coalesce(overs_limit, F.lit(20)) * F.coalesce(balls_per_over, F.lit(6))
    reduced = F.floor(target_overs) * 6 + F.round((target_overs - F.floor(target_overs)) * 10)
    use = F.when(target_overs.isNotNull() & (reduced < full), reduced).otherwise(full)
    return F.least(use, F.lit(TOTAL_BALLS)).cast("int")


def rated_matches(spark: SparkSession, t: Tables) -> DataFrame:
    """Matches with a result (win or tie), with what the ball values need."""
    i2 = spark.table(t.silver_innings).filter("innings_no = 2").select("match_id", "target_overs")
    m = spark.table(t.silver_matches).filter(F.col("result").isin("win", "tie"))
    return m.join(i2, "match_id", "left").select(
        "match_id",
        competition_col().alias("competition"),
        F.year("match_date").alias("year"),
        F.trim(F.element_at(F.split("venue", ","), 1)).alias("venue"),
        allotted_balls(F.col("target_overs"), F.col("overs_limit"), F.col("balls_per_over")).alias("allotted"),
    )


def ball_values(deliveries: DataFrame, rated: DataFrame, re: DataFrame,
                env: DataFrame, venues: DataFrame) -> DataFrame:
    """One row per ball of innings 1 and 2: the state before and after, and its value."""
    d = deliveries.filter("innings_no IN (1, 2) AND NOT is_super_over").join(rated, "match_id")
    in_order = Window.partitionBy("match_id", "innings_no").orderBy("seq")
    before = in_order.rowsBetween(Window.unboundedPreceding, -1)
    legal = F.col("is_legal").cast("int")

    def clamp(c, hi):
        return F.greatest(F.least(c, F.lit(hi)), F.lit(0)).cast("int")

    d = (d.withColumn("w_before", clamp(F.coalesce(F.sum("dismissals").over(before), F.lit(0)), MAX_WICKETS))
          .withColumn("w_after", clamp(F.col("w_before") + F.col("dismissals"), MAX_WICKETS))
          .withColumn("b_before", clamp(F.col("allotted") - (F.col("legal_ball_no") - legal), TOTAL_BALLS))
          .withColumn("b_after", clamp(F.col("allotted") - F.col("legal_ball_no"), TOTAL_BALLS)))

    d = (d.join(F.broadcast(env.select("competition", "year", F.col("factor").alias("env_factor"))),
                ["competition", "year"], "left")
          .join(F.broadcast(venues.select("venue", F.col("factor").alias("venue_factor"))), "venue", "left")
          .withColumn("scale", F.coalesce("env_factor", F.lit(1.0)) * F.coalesce("venue_factor", F.lit(1.0))))

    def re_as(b, w, name):
        return F.broadcast(re.select(F.col("balls_left").alias(b), F.col("wickets").alias(w),
                                     F.col("expected_runs").alias(name)))

    d = (d.join(re_as("b_before", "w_before", "re0"), ["b_before", "w_before"], "left")
          .join(re_as("b_after", "w_before", "re1"), ["b_after", "w_before"], "left")
          .join(re_as("b_after", "w_after", "re2"), ["b_after", "w_after"], "left"))

    e0, e1, e2 = (F.col(c) * F.col("scale") for c in ("re0", "re1", "re2"))
    return d.select(
        "match_id", "innings_no", "seq", "competition", "year", "venue",
        "batter_id", "non_striker_id", "bowler_id", "is_legal",
        "runs_batter", "runs_extras", "runs_total", "extras_wides", "extras_noballs",
        "extras_byes", "extras_legbyes", "wickets", "dismissals",
        "b_before", "w_before", "b_after", "w_after", "scale",
        F.round(e0, 4).alias("expected_before"),
        F.round(e2, 4).alias("expected_after"),
        F.round(e1 - e0, 4).alias("ball_cost"),
        F.round(e2 - e1, 4).alias("wicket_cost"),
        F.round(F.col("runs_total") + e2 - e0, 4).alias("runs_added"),
    )


def _credit(df: DataFrame, player: Column, **values: Column) -> DataFrame:
    cols = [F.coalesce(values.get(name, F.lit(0)), F.lit(0)).cast(kind).alias(name)
            for name, kind in NUMBERS.items()]
    return df.select("match_id", player.alias("player_id"), *cols)


def player_impact(bv: DataFrame, match_players: DataFrame) -> DataFrame:
    """Sum every player's credits per match. Every listed player gets a row, even with no involvement."""
    not_wide = (F.col("extras_wides") == 0).cast("int")
    conceded = F.col("runs_batter") + F.col("extras_wides") + F.col("extras_noballs")

    striker = _credit(bv, F.col("batter_id"),
                      batting=F.col("runs_batter") + F.col("ball_cost"),
                      runs=F.col("runs_batter"), balls_faced=not_wide)
    bowler = _credit(bv, F.col("bowler_id"),
                     bowling=-conceded - F.col("ball_cost"),
                     balls_bowled=F.col("is_legal").cast("int"), runs_conceded=conceded)

    wk = (bv.filter("dismissals > 0")
            .select("match_id", "bowler_id", "wicket_cost", "dismissals", F.explode("wickets").alias("wk"))
            .filter(~F.col("wk.kind").isin(*NOT_DISMISSALS))
            .withColumn("cost", F.col("wicket_cost") / F.col("dismissals"))
            .withColumn("bshare", _lookup(BOWLER_SHARE, F.col("wk.kind")))
            .withColumn("fshare", _lookup(FIELDER_SHARE, F.col("wk.kind"))))
    out = _credit(wk, F.col("wk.player_out_id"), batting=F.col("cost"), dismissed=F.lit(1))
    taker = _credit(wk.filter("bshare > 0"), F.col("bowler_id"),
                    bowling=-F.col("cost") * F.col("bshare"), wickets=F.lit(1))
    fielders = (wk.filter((F.col("fshare") > 0) & (F.size("wk.fielder_ids") > 0))
                  .withColumn("n", F.size("wk.fielder_ids"))
                  .withColumn("fielder", F.explode("wk.fielder_ids")))
    kind = F.col("wk.kind")
    field = _credit(fielders, F.col("fielder"),
                    fielding=-F.col("cost") * F.col("fshare") / F.col("n"),
                    catches=(kind == "caught").cast("int"),
                    stumpings=(kind == "stumped").cast("int"),
                    run_outs=(kind == "run out").cast("int"))

    credits = striker.unionByName(bowler).unionByName(out).unionByName(taker).unionByName(field)
    totals = (credits.filter(F.col("player_id").isNotNull())
              .groupBy("match_id", "player_id")
              .agg(*[F.sum(c).alias(c) for c in NUMBERS]))

    xi = match_players.join(bv.select("match_id").distinct(), "match_id").select("match_id", "player_id", "team")
    full = xi.join(totals, ["match_id", "player_id"], "full_outer").filter(F.col("player_id").isNotNull())
    filled = full.select(
        "match_id", "player_id", "team",
        *[F.coalesce(F.col(c), F.lit(0)).cast(k).alias(c) for c, k in NUMBERS.items()])
    return filled.withColumn(
        "total_impact", F.round(F.col("batting") + F.col("bowling") + F.col("fielding"), 3)
    ).select("match_id", "player_id", "team",
             *[F.round(c, 3).alias(c) if NUMBERS[c] == "double" else c for c in NUMBERS],
             "total_impact")


def run_ratings(spark: SparkSession, t: Tables, version: str) -> dict:
    """Value every ball and credit players for one model version (re-running replaces it)."""
    def of_version(table):
        return spark.table(table).filter(F.col("version") == version).drop("version")

    re = of_version(t.gold_run_expectancy)
    if re.limit(1).count() == 0:
        raise ValueError(f"No run-expectancy table for model version {version}: run 05_gold_model first")

    bv = ball_values(spark.table(t.silver_deliveries), rated_matches(spark, t), re,
                     of_version(t.gold_environment_factors), of_version(t.gold_venue_factors))
    key = spark.createDataFrame([(version,)], "version string")
    bv = bv.withColumn("version", F.lit(version))
    storage.replace_rows(spark, t.gold_ball_values, bv, key, "version")

    stored = of_version(t.gold_ball_values)
    impact = player_impact(stored, spark.table(t.silver_match_players)).withColumn("version", F.lit(version))
    storage.replace_rows(spark, t.gold_player_match_impact, impact, key, "version")
    return {"version": version,
            "balls_valued": stored.count(),
            "player_matches": of_version(t.gold_player_match_impact).count()}
