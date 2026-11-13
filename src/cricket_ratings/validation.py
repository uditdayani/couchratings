"""Model validation: does the rating system behave like a sensible cricket rating?

Each check is a pure function over DataFrames (so it can be tested on small data) and
returns result rows: check, scope, value, target, passed, detail. `run_validation`
applies them to one model version's Gold tables.
"""
from typing import List, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Tables

CALIBRATION_MAX_RUNS = 3.0      # |avg first-innings runs added| per competition, since CALIBRATION_FROM
CALIBRATION_FROM = 2019
CALIBRATION_MIN_INNINGS = 50
MEDIAN_RANGE = (6.2, 6.8)
TOP_TAIL_RANGE = (0.5, 2.0)     # % of ratings at 9.3+
ROLE_RANGE = (25.0, 55.0)       # % of 9+ ratings driven mainly by bowling
POTM_TOP3_MIN = 70.0            # % of matches where the player of the match is top 3
SPOT_MIN_RATING = 9.0
SPOT_MIN_SHARE = 0.8

# Famous performances that should rate 9+. (label, date, name LIKE pattern, stat, value).
# The stat pins down the right player and match. Gayle, McCullum and Herath also appear in
# the model's own top-20, so they are not independent of it; the rest were chosen beforehand.
FAMOUS: List[Tuple[str, str, str, str, int]] = [
    ("Rohit Sharma 118 v Sri Lanka, Indore", "2017-12-22", "%Sharma%", "runs", 118),
    ("Glenn Maxwell 145* v Sri Lanka, Pallekele", "2016-09-06", "%Maxwell%", "runs", 145),
    ("Aaron Finch 172 v Zimbabwe, Harare", "2018-07-03", "%Finch%", "runs", 172),
    ("Yuvraj Singh 58 off 16 (six sixes) v England, Durban", "2007-09-19", "%Yuvraj%", "runs", 58),
    ("Ajantha Mendis 6/8 v Zimbabwe, Hambantota", "2012-09-18", "%Mendis%", "wickets", 6),
    ("Deepak Chahar 6/7 v Bangladesh, Nagpur", "2019-11-10", "%Chahar%", "wickets", 6),
    ("Alzarri Joseph 6/12 for Mumbai Indians v Sunrisers", "2019-04-06", "%Joseph%", "wickets", 6),
    ("Chris Gayle 175* for RCB v Pune Warriors (not independent)", "2013-04-23", "%Gayle%", "runs", 175),
    ("Brendon McCullum 158* in the first IPL match (not independent)", "2008-04-18", "%McCullum%", "runs", 158),
    ("Rangana Herath 5/3 v New Zealand, World T20 (not independent)", "2014-03-31", "%Herath%", "wickets", 5),
]


def _row(check, scope, value, target, passed, detail=""):
    return {"check": check, "scope": scope, "value": None if value is None else float(value),
            "target": target, "passed": bool(passed), "detail": detail}


def calibration(ball_values: DataFrame) -> List[dict]:
    """First-innings runs added should average near zero in each competition."""
    per_innings = (ball_values.filter((F.col("innings_no") == 1) & (F.col("year") >= CALIBRATION_FROM))
                   .groupBy("match_id", "competition").agg(F.sum("runs_added").alias("ra")))
    rows = (per_innings.groupBy("competition").agg(F.count("*").alias("n"), F.avg("ra").alias("avg"))
            .filter(F.col("n") >= CALIBRATION_MIN_INNINGS).orderBy("competition").collect())
    return [_row("calibration", r.competition, round(r.avg, 2), f"|avg| <= {CALIBRATION_MAX_RUNS} runs",
                 abs(r.avg) <= CALIBRATION_MAX_RUNS, f"{r.n} first innings since {CALIBRATION_FROM}")
            for r in rows]


def scale(ratings: DataFrame) -> List[dict]:
    """Every competition's ratings should have the same shape: median ~6.5, ~1% at 9.3+."""
    rows = (ratings.filter("involved").groupBy("competition").agg(
        F.count("*").alias("n"),
        F.percentile_approx("rating", 0.5).alias("median"),
        (100 * F.avg((F.col("rating") >= 9.3).cast("double"))).alias("tail"),
    ).orderBy("competition").collect())
    out = []
    for r in rows:
        lo, hi = MEDIAN_RANGE
        out.append(_row("median_rating", r.competition, r.median, f"{lo}-{hi}", lo <= r.median <= hi,
                        f"{r.n} rated performances"))
        lo, hi = TOP_TAIL_RANGE
        out.append(_row("pct_9_3_plus", r.competition, round(r.tail, 2), f"{lo}-{hi}%", lo <= r.tail <= hi))
    return out


def role_balance(ratings: DataFrame) -> List[dict]:
    """Neither batters nor bowlers should monopolise the 9+ ratings."""
    rows = (ratings.filter(F.col("rating") >= 9).groupBy("competition").agg(
        F.count("*").alias("n"),
        (100 * F.avg((F.col("bowling") > F.col("batting")).cast("double"))).alias("pct"),
    ).orderBy("competition").collect())
    lo, hi = ROLE_RANGE
    return [_row("pct_9_plus_mainly_bowling", r.competition, round(r.pct, 1), f"{lo:.0f}-{hi:.0f}%",
                 lo <= r.pct <= hi, f"{r.n} ratings of 9+") for r in rows]


def player_of_match(ratings: DataFrame, matches: DataFrame) -> List[dict]:
    """The official player of the match should usually be among the top 3 rated."""
    ranked = ratings.filter("involved").withColumn(
        "rk", F.rank().over(Window.partitionBy("match_id").orderBy(F.col("rating").desc(),
                                                                   F.col("total_impact").desc())))
    pom = matches.select("match_id", F.explode("player_of_match_ids").alias("player_id"))
    best = pom.join(ranked, ["match_id", "player_id"]).groupBy("match_id").agg(F.min("rk").alias("rk"))
    r = best.agg(F.count("*").alias("n"),
                 (100 * F.avg((F.col("rk") == 1).cast("double"))).alias("top1"),
                 (100 * F.avg((F.col("rk") <= 3).cast("double"))).alias("top3")).first()
    if not r.n:
        return [_row("player_of_match_top3", "All", None, f">= {POTM_TOP3_MIN:.0f}%", False, "no matches")]
    return [_row("player_of_match_top3", "All", round(r.top3, 1), f">= {POTM_TOP3_MIN:.0f}%",
                 r.top3 >= POTM_TOP3_MIN, f"{r.n} matches; top-rated in {r.top1:.1f}%")]


def famous_performances(spark: SparkSession, ratings: DataFrame, players: DataFrame,
                        spots: Sequence[tuple] = FAMOUS) -> Tuple[List[dict], DataFrame]:
    """Each famous performance should rate SPOT_MIN_RATING+. Returns the summary row and
    the per-performance table (rating is null when the match or player wasn't found)."""
    s = spark.createDataFrame(list(spots), "label string, d string, name_like string, stat string, value int") \
        .withColumn("spot_date", F.to_date("d")).drop("d")
    found = (ratings.join(players.select("player_id", "name"), "player_id")
             .select("match_date", "name", "rating", "runs", "wickets", "competition"))
    cond = F.expr("""match_date = spot_date AND name LIKE name_like AND
                     ((stat = 'runs' AND runs = value) OR (stat = 'wickets' AND wickets = value))""")
    joined = s.join(found, cond, "left").select(
        "label", F.col("spot_date").alias("match_date"), "name", "competition", "rating")
    table = joined.withColumn("passed", F.col("rating") >= SPOT_MIN_RATING).orderBy("match_date")
    rows = table.collect()
    hits = [r for r in rows if r.rating is not None]
    share = sum(r.passed for r in hits) / len(hits) if hits else 0.0
    misses = [r.label for r in rows if r.rating is not None and not r.passed]
    missing = [r.label for r in rows if r.rating is None]
    detail = f"{sum(r.passed for r in hits)} of {len(hits)} found rate {SPOT_MIN_RATING}+"
    if misses:
        detail += "; below: " + "; ".join(misses)
    if missing:
        detail += "; not found: " + "; ".join(missing)
    return [_row("famous_performances", "All", round(share * 100, 1),
                 f">= {SPOT_MIN_SHARE:.0%} rate {SPOT_MIN_RATING}+", bool(hits) and share >= SPOT_MIN_SHARE,
                 detail)], table


def run_validation(spark: SparkSession, t: Tables, version: str) -> Tuple[List[dict], DataFrame]:
    def of_version(table):
        return spark.table(table).filter(F.col("version") == version)

    ratings = of_version(t.gold_player_match_ratings)
    if ratings.limit(1).count() == 0:
        raise ValueError(f"No ratings for model version {version}: run 07_gold_scale_form first")
    results = (calibration(of_version(t.gold_ball_values)) + scale(ratings) + role_balance(ratings)
               + player_of_match(ratings, spark.table(t.silver_matches)))
    spot_rows, spot_table = famous_performances(spark, ratings, spark.table(t.silver_players))
    return results + spot_rows, spot_table


def save_and_enforce(spark: SparkSession, t: Tables, results: List[dict], version: str, run_id: str) -> None:
    """Append to ops.validation_results, then raise if any check failed."""
    df = spark.createDataFrame(results, "check string, scope string, value double, target string, "
                                        "passed boolean, detail string")
    storage.append(df.withColumn("version", F.lit(version)).withColumn("run_id", F.lit(run_id))
                     .withColumn("checked_at", F.current_timestamp()), t.validation_results)
    failed = [f"{r['check']} ({r['scope']})" for r in results if not r["passed"]]
    if failed:
        raise RuntimeError("Validation failed: " + ", ".join(failed))
