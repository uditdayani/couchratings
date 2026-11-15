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
RAIN_MAX_RUNS = 5.0            # |avg first-innings runs added| in rain-affected matches
MAX_WICKET_COST = 25.0         # runs; a single dismissal shouldn't cost more...
MAX_COSTLY_PER_10K = 1.0       # ...except in fewer than 1 in 10,000 dismissals
ERA_MAX_RANGE = 2.5            # percentage points between seasons' shares of 9+ ratings
ERA_MIN_RATED = 1000           # only seasons with enough rated performances
STAGE_MAX_SPREAD_RATIO = 1.3   # widest / narrowest rating spread across batting stages
STAGE_MAX_SHARE_RATIO = 2.0    # highest / lowest share of 9+ (and of 4 or less) across stages
STAGE_MIN_INNINGS = 500
MEDIAN_RANGE = (6.2, 6.8)
TOP_TAIL_RANGE = (0.5, 2.0)     # % of ratings at 9.3+
ROLE_FAIR_MARGIN = 5.0         # % of 9+ mainly bowling: within this many points of bowling's fair share
BOTH_ROLES_MAX_RATIO = 1.3      # batted-and-bowled: share at 9+ (or 4-) / share of all ratings
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


def rain_calibration(ball_values: DataFrame, matches: DataFrame) -> List[dict]:
    """Rain-affected first innings (a method such as D/L recorded) should average near zero
    runs added, like normal ones. V1 averaged +30.5 because a reduced chase also shortened
    a completed first innings."""
    rain = matches.filter(F.col("method").isNotNull()).select("match_id")
    per_innings = (ball_values.filter(F.col("innings_no") == 1).join(rain, "match_id")
                   .groupBy("match_id").agg(F.sum("runs_added").alias("ra")))
    r = per_innings.agg(F.count("*").alias("n"), F.avg("ra").alias("avg")).first()
    if not r.n:
        return [_row("rain_calibration", "All", None, f"|avg| <= {RAIN_MAX_RUNS} runs", True,
                     "no rain-affected matches")]
    return [_row("rain_calibration", "All", round(r.avg, 2), f"|avg| <= {RAIN_MAX_RUNS} runs",
                 abs(r.avg) <= RAIN_MAX_RUNS, f"{r.n} rain-affected first innings")]


def wicket_costs(ball_values: DataFrame) -> List[dict]:
    """No single dismissal should cost an implausible number of runs. V1's hold-flat
    smoothing let 8 of ~61,000 cost over 25 (worst: 38.5, at 4 down with 105 balls left)."""
    d = ball_values.filter(F.col("dismissals") > 0)
    r = d.agg(F.count("*").alias("n"),
              F.sum((F.col("wicket_cost") < -MAX_WICKET_COST).cast("int")).alias("costly"),
              F.min("wicket_cost").alias("worst")).first()
    per_10k = 10000 * (r.costly or 0) / r.n if r.n else 0.0
    return [_row("costly_dismissals_per_10k", "All", round(per_10k, 2),
                 f"< {MAX_COSTLY_PER_10K} per 10,000 cost over {MAX_WICKET_COST:.0f} runs",
                 per_10k < MAX_COSTLY_PER_10K,
                 f"{r.costly or 0} of {r.n} dismissals; worst {r.worst:.1f} runs" if r.n else "no dismissals")]


def era_flatness(ratings: DataFrame) -> List[dict]:
    """The share of 9+ ratings should be roughly the same every season. V1 ranged from about
    2% to 5% because high-scoring modern seasons produce bigger swings in runs."""
    yearly = (ratings.filter("involved").groupBy("competition", F.year("match_date").alias("year"))
              .agg(F.count("*").alias("n"), (100 * F.avg((F.col("rating") >= 9).cast("double"))).alias("pct"))
              .filter(F.col("n") >= ERA_MIN_RATED))
    rows = (yearly.groupBy("competition").agg(F.count("*").alias("years"), F.min("pct").alias("lo"),
                                              F.max("pct").alias("hi"))
            .filter("years >= 3").orderBy("competition").collect())
    return [_row("era_9_plus_range", r.competition, round(r.hi - r.lo, 2), f"<= {ERA_MAX_RANGE} points",
                 r.hi - r.lo <= ERA_MAX_RANGE,
                 f"{r.years} seasons with {ERA_MIN_RATED}+ ratings: {r.lo:.1f}% to {r.hi:.1f}% rated 9+")
            for r in rows]


def _pure_batters(ratings: DataFrame) -> DataFrame:
    return ratings.filter(F.col("involved") & (F.col("balls_bowled") == 0)
                          & ((F.col("balls_faced") > 0) | (F.col("dismissed") > 0)))


def stage_balance(ratings: DataFrame) -> List[dict]:
    """Batters who come in late shouldn't be squeezed into the middle of the scale. V1, for
    pure batters: spread 1.56 for openers vs 0.62 for finishers; 4-or-less 9.8% vs 0.1%;
    9+ 5.3% vs 0.6%."""
    if "batting_stage" not in ratings.columns:
        return [_row("stage_spread_ratio", "All", None, f"<= {STAGE_MAX_SPREAD_RATIO}", False,
                     "ratings have no batting_stage (built before V2.1)")]
    st = (_pure_batters(ratings).filter(F.col("batting_stage").isNotNull())
          .groupBy("batting_stage").agg(
              F.count("*").alias("n"), F.stddev("rating").alias("sd"),
              (100 * F.avg((F.col("rating") >= 9).cast("double"))).alias("top"),
              (100 * F.avg((F.col("rating") <= 4).cast("double"))).alias("low"))
          .filter(F.col("n") >= STAGE_MIN_INNINGS).orderBy("batting_stage").collect())
    if len(st) < 2:
        return [_row("stage_spread_ratio", "All", None, f"<= {STAGE_MAX_SPREAD_RATIO}", False,
                     "fewer than two batting stages with enough innings")]
    detail = "; ".join(f"{r.batting_stage}: spread {r.sd:.2f}, 9+ {r.top:.1f}%, 4- {r.low:.1f}%" for r in st)

    def ratio(vals):
        lo = min(vals)
        return float("inf") if lo == 0 else max(vals) / lo

    out = []
    for name, vals, limit in (("stage_spread_ratio", [r.sd for r in st], STAGE_MAX_SPREAD_RATIO),
                              ("stage_9_plus_ratio", [r.top for r in st], STAGE_MAX_SHARE_RATIO),
                              ("stage_4_or_less_ratio", [r.low for r in st], STAGE_MAX_SHARE_RATIO)):
        v = ratio(vals)
        out.append(_row(name, "All", None if v == float("inf") else round(v, 2), f"<= {limit}",
                        v <= limit, detail))
    return out


def cameo_vs_anchor(ratings: DataFrame) -> List[dict]:
    """A quick cameo (15+ runs off 8 balls or fewer) should rate at least as well, on
    average, as a steady 35-45 off 26-32 balls. Pure batters only."""
    b = _pure_batters(ratings)
    cameo = b.filter((F.col("runs") >= 15) & (F.col("balls_faced") > 0) & (F.col("balls_faced") <= 8))
    anchor = b.filter(F.col("runs").between(35, 45) & F.col("balls_faced").between(26, 32))
    c = cameo.agg(F.count("*").alias("n"), F.avg("rating").alias("avg")).first()
    a = anchor.agg(F.count("*").alias("n"), F.avg("rating").alias("avg")).first()
    if c.n < 20 or a.n < 20:
        return [_row("cameo_vs_anchor", "All", None, "cameo avg >= anchor avg", True,
                     f"not enough innings to compare ({c.n} cameos, {a.n} anchors)")]
    return [_row("cameo_vs_anchor", "All", round(c.avg - a.avg, 2), "cameo avg >= anchor avg",
                 c.avg >= a.avg, f"{c.n} cameos avg {c.avg:.2f}; {a.n} anchor innings avg {a.avg:.2f}")]


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


def _roles(ratings: DataFrame) -> DataFrame:
    return (ratings.filter("involved")
            .withColumn("_bowled", F.col("balls_bowled") >= 6)
            .withColumn("_batted", (F.col("balls_faced") > 0) | (F.col("dismissed") > 0)))


def role_balance(ratings: DataFrame) -> List[dict]:
    """Bowling's share of 9+ ratings should match its share of performances: with batting
    ranked against batting and bowling against bowling, any innings or spell (an over or
    more) should be about equally likely to reach 9. Fair share = spells / (spells + innings)."""
    r = _roles(ratings)
    rows = r.groupBy("competition").agg(
        F.sum(F.col("_bowled").cast("int")).alias("spells"),
        F.sum(F.col("_batted").cast("int")).alias("innings"),
        F.sum((F.col("rating") >= 9).cast("int")).alias("n"),
        (100 * F.avg(F.when(F.col("rating") >= 9, (F.col("bowling") > F.col("batting")).cast("double")))).alias("pct"),
    ).orderBy("competition").collect()
    out = []
    for x in rows:
        if not x.n:
            continue
        fair = 100.0 * x.spells / max(x.spells + x.innings, 1)
        out.append(_row("pct_9_plus_mainly_bowling", x.competition, round(x.pct, 1),
                        f"fair share {fair:.1f}% +- {ROLE_FAIR_MARGIN:.0f}",
                        abs(x.pct - fair) <= ROLE_FAIR_MARGIN, f"{x.n} ratings of 9+"))
    return out


def both_roles(ratings: DataFrame) -> List[dict]:
    """Players who batted and bowled shouldn't crowd the extremes: their share of 9+ ratings,
    and of 4 or less, over their share of all ratings."""
    r = _roles(ratings).withColumn("_both", (F.col("_bowled") & F.col("_batted")).cast("double"))
    rows = r.groupBy("competition").agg(
        F.avg("_both").alias("all"),
        F.avg(F.when(F.col("rating") >= 9, F.col("_both"))).alias("top"),
        F.avg(F.when(F.col("rating") <= 4, F.col("_both"))).alias("low"),
    ).orderBy("competition").collect()
    out = []
    for x in rows:
        if not x.all or x.top is None or x.low is None:
            continue
        ratio = max(x.top, x.low) / x.all
        out.append(_row("both_roles_extremes_ratio", x.competition, round(ratio, 2),
                        f"<= {BOTH_ROLES_MAX_RATIO}", ratio <= BOTH_ROLES_MAX_RATIO,
                        f"batted and bowled: {100 * x.all:.1f}% of ratings, {100 * x.top:.1f}% of 9+, "
                        f"{100 * x.low:.1f}% of 4 or less"))
    return out


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
    results = (calibration(of_version(t.gold_ball_values))
               + rain_calibration(of_version(t.gold_ball_values), spark.table(t.silver_matches))
               + wicket_costs(of_version(t.gold_ball_values))
               + scale(ratings) + era_flatness(ratings) + role_balance(ratings) + both_roles(ratings)
               + stage_balance(ratings) + cameo_vs_anchor(ratings)
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
