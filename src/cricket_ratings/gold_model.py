"""Gold model tables: the run-expectancy (RE) table and venue factors.

RE(b, w) = average runs a side scores from a point in its innings to the end,
with b legal balls left and w wickets down. It is fitted on FIRST innings only:
a chase stops when the target is reached, which would cut "runs to the end"
short and bias the table. The same table is used for both innings in V1.

Expected runs in a given match = RE(b, w) x environment factor (how high-scoring that
competition was that year) x venue factor (how high-scoring the ground is within its
competition). Week 4 applies them.

Everything is stored under a model version, so ratings can always be traced
back to (and rebuilt from) the exact table that produced them.
"""
import json
from typing import Optional

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Tables

TOTAL_BALLS = 120   # 20 overs x 6 balls
MAX_WICKETS = 10


# ------------------------------------------------------------------ inputs

def completed_matches(spark: SparkSession, t: Tables, min_date: Optional[str] = None) -> DataFrame:
    """Full-length 20-over matches that finished normally (no rain method, no penalty runs).

    Excludes no-results, shortened chases (target overs below the limit) and
    innings with a miscounted over (more than 120 legal balls).
    """
    date_filter = f"AND m.match_date >= DATE'{min_date}'" if min_date else ""
    return spark.sql(f"""
        SELECT m.match_id, m.match_date, m.venue
        FROM {t.silver_matches} m
        JOIN {t.silver_innings} i1
          ON i1.match_id = m.match_id AND i1.innings_no = 1 AND NOT i1.is_super_over
        LEFT JOIN {t.silver_innings} i2
          ON i2.match_id = m.match_id AND i2.innings_no = 2
        WHERE m.result IN ('win', 'tie') AND m.method IS NULL
          AND m.overs_limit = 20 AND m.balls_per_over = 6
          AND i1.legal_balls <= {TOTAL_BALLS}
          AND i1.penalty_runs_pre + i1.penalty_runs_post = 0
          AND (i2.target_overs IS NULL OR i2.target_overs = m.overs_limit)
          {date_filter}""")


def re_observations(spark: SparkSession, t: Tables, matches: DataFrame) -> DataFrame:
    """One row per first-innings state (balls left, wickets down) the first time
    each innings reaches it, with the runs scored from there to the end."""
    d = (spark.table(t.silver_deliveries).filter("innings_no = 1 AND NOT is_super_over")
         .join(matches.select("match_id"), "match_id"))
    in_order = Window.partitionBy("match_id").orderBy("seq")
    before = in_order.rowsBetween(Window.unboundedPreceding, -1)
    whole = Window.partitionBy("match_id")

    obs = d.select(
        "match_id", "seq",
        (F.lit(TOTAL_BALLS) - (F.col("legal_ball_no") - F.col("is_legal").cast("int")))
        .cast("int").alias("balls_left"),
        F.coalesce(F.sum("dismissals").over(before), F.lit(0)).cast("int").alias("wickets"),
        (F.sum("runs_total").over(whole) - F.coalesce(F.sum("runs_total").over(before), F.lit(0)))
        .cast("int").alias("runs_to_end"),
    ).filter(F.col("wickets") < MAX_WICKETS)

    first = Window.partitionBy("match_id", "balls_left", "wickets").orderBy("seq")
    return obs.withColumn("_rn", F.row_number().over(first)).filter("_rn = 1").drop("_rn", "seq")


def raw_cells(obs: DataFrame) -> pd.DataFrame:
    """Small (at most 121 x 10 rows): sample size and mean per state, as pandas."""
    return (obs.groupBy("balls_left", "wickets")
            .agg(F.count("*").alias("n"), F.sum("runs_to_end").alias("runs_sum"))
            .toPandas())


# --------------------------------------------------------------- smoothing

def _local_linear(b: int, xs: np.ndarray, ns: np.ndarray, sums: np.ndarray) -> Optional[float]:
    """Value at `b` of the straight line through the cells' means, weighted by sample size.
    None when fewer than two cells have data (a line needs two points)."""
    has = ns > 0
    if has.sum() < 2:
        return None
    x, w, y = xs[has] - b, ns[has], sums[has] / ns[has]
    sw, sx, sy = w.sum(), (w * x).sum(), (w * y).sum()
    sxx, sxy = (w * x * x).sum(), (w * x * y).sum()
    denom = sw * sxx - sx * sx
    return float((sxx * sy - sx * sxy) / denom) if denom > 0 else None


def smooth_re(raw: pd.DataFrame, min_samples: int = 50, max_window: int = 12) -> pd.DataFrame:
    """Turn raw cell averages into a complete, smooth, sensible RE table.

    1. Pool: a cell with fewer than `min_samples` innings borrows from neighbouring
       balls-left cells (same wickets), widening up to +/-`max_window` balls. It fits a
       weighted straight line through them rather than averaging, so neighbours on
       only one side (a gap, or the 120-ball edge) don't drag the estimate.
    2. Interpolate: cells still empty are interpolated along balls left, anchored
       at 0 runs with 0 balls left (beyond the last observed cell, held flat).
    3. Make it monotone: more balls left never means fewer runs, and more wickets
       down never means more runs.
    Terminal states (0 balls left, or 10 wickets) are exactly 0.
    """
    balls = np.arange(TOTAL_BALLS + 1)
    n = np.zeros((TOTAL_BALLS + 1, MAX_WICKETS))
    s = np.zeros((TOTAL_BALLS + 1, MAX_WICKETS))
    for r in raw.itertuples(index=False):
        if 0 <= r.balls_left <= TOTAL_BALLS and 0 <= r.wickets < MAX_WICKETS:
            n[r.balls_left, r.wickets] += r.n
            s[r.balls_left, r.wickets] += r.runs_sum

    est = np.full(n.shape, np.nan)
    method = np.full(n.shape, "interpolated", dtype=object)
    for w in range(MAX_WICKETS):
        for b in range(1, TOTAL_BALLS + 1):
            if n[b, w] >= min_samples:
                est[b, w], method[b, w] = s[b, w] / n[b, w], "observed"
                continue
            for h in range(1, max_window + 1):
                lo, hi = max(1, b - h), min(TOTAL_BALLS, b + h)
                fit = _local_linear(b, np.arange(lo, hi + 1), n[lo:hi + 1, w], s[lo:hi + 1, w])
                if fit is not None and n[lo:hi + 1, w].sum() >= min_samples:
                    est[b, w], method[b, w] = fit, "pooled"
                    break
        known = ~np.isnan(est[:, w])
        known[0] = True
        est[0, w] = 0.0
        est[:, w] = np.interp(balls, balls[known], est[known, w])
        est[:, w] = np.maximum.accumulate(est[:, w])          # non-decreasing in balls left
    est = np.minimum.accumulate(est, axis=1)                   # non-increasing in wickets
    method[0, :] = "terminal"

    rows = [{"balls_left": int(b), "wickets": int(w), "expected_runs": round(float(est[b, w]), 3),
             "sample_size": int(n[b, w]), "method": method[b, w]}
            for b in balls for w in range(MAX_WICKETS)]
    rows += [{"balls_left": int(b), "wickets": MAX_WICKETS, "expected_runs": 0.0,
              "sample_size": 0, "method": "terminal"} for b in balls]
    return pd.DataFrame(rows).sort_values(["wickets", "balls_left"], ignore_index=True)


# --------------------------------------------- scoring environment and venues

def _shrink(raw, matches, prior: int):
    """Pull a ratio towards 1.0 when it rests on few matches."""
    weight = matches / (matches + F.lit(prior))
    return F.lit(1.0) + weight * (raw - F.lit(1.0))


def match_rates(spark: SparkSession, t: Tables, matches: DataFrame) -> DataFrame:
    """Runs and legal balls per match (first innings, and both innings), with the
    match's competition, year and venue.

    Competition: "T20I" for internationals, otherwise the event name (e.g. "Indian
    Premier League"). Venue names are grouped on the part before the first comma,
    because Cricsheet sometimes adds the city ("Wankhede Stadium, Mumbai").
    """
    d = (spark.table(t.silver_deliveries).filter("NOT is_super_over")
         .join(matches.select("match_id"), "match_id"))
    legal = F.col("is_legal").cast("int")
    first = F.col("innings_no") == 1
    per_match = d.groupBy("match_id").agg(
        F.sum(F.when(first, F.col("runs_total")).otherwise(0)).alias("runs_1"),
        F.sum(F.when(first, legal).otherwise(0)).alias("balls_1"),
        F.sum("runs_total").alias("runs_all"),
        F.sum(legal).alias("balls_all"),
    )
    m = spark.table(t.silver_matches).select(
        "match_id",
        F.when(F.col("team_type") == "international", F.lit("T20I"))
         .otherwise(F.coalesce(F.col("event_name"), F.lit("Unknown"))).alias("competition"),
        F.year("match_date").alias("year"),
        F.trim(F.element_at(F.split("venue", ","), 1)).alias("venue"),
    )
    return per_match.join(m, "match_id").filter("balls_1 > 0 AND balls_all > 0")


def environment_factors(rates: DataFrame, baseline_rate: float, prior_matches: int = 5) -> DataFrame:
    """How high-scoring each competition was in each year, relative to the matches the
    run-expectancy table was fitted on (first-innings runs per ball). Scales RE so IPL
    batters are judged against IPL scoring, and 2010 against 2010."""
    g = rates.groupBy("competition", "year").agg(
        F.count("*").alias("matches"),
        F.sum("runs_1").alias("runs_1"), F.sum("balls_1").alias("balls_1"),
        F.sum("runs_all").alias("runs_all"), F.sum("balls_all").alias("balls_all"),
    )
    raw = F.col("runs_1") / F.col("balls_1") / F.lit(baseline_rate)
    return g.select(
        "competition", "year", "matches",
        F.round(F.col("runs_1") / F.col("matches"), 1).alias("avg_first_innings"),
        F.round(raw, 4).alias("raw_factor"),
        F.round(_shrink(raw, F.col("matches"), prior_matches), 4).alias("factor"),
        (F.col("runs_all") / F.col("balls_all")).alias("all_innings_rate"),
    )


def venue_factors(rates: DataFrame, env: DataFrame, prior_matches: int = 10) -> DataFrame:
    """Each venue's scoring compared with what its matches' competition and year would
    predict (both innings), so a ground isn't "high-scoring" just for hosting the IPL."""
    joined = rates.join(env.select("competition", "year", "all_innings_rate"), ["competition", "year"])
    v = joined.groupBy("venue").agg(
        F.count("*").alias("matches"),
        F.sum("runs_all").alias("runs"),
        F.sum("balls_all").alias("legal_balls"),
        F.sum(F.col("balls_all") * F.col("all_innings_rate")).alias("expected_runs"),
    )
    raw = F.col("runs") / F.col("expected_runs")
    return v.select(
        "venue", "matches", "legal_balls",
        F.round(F.col("runs") / F.col("legal_balls") * 6, 2).alias("runs_per_over"),
        F.round(raw, 4).alias("raw_factor"),
        F.round(_shrink(raw, F.col("matches"), prior_matches), 4).alias("factor"),
    )


# --------------------------------------------------------------------- fit

def fit_model(spark: SparkSession, t: Tables, version: str, min_date: Optional[str] = None,
              min_samples: int = 50, max_window: int = 12, venue_prior_matches: int = 10,
              environment_prior_matches: int = 5, notes: str = "") -> dict:
    """Fit RE, environment and venue factors and store them under `version`
    (re-fitting replaces it).

    The RE table's shape is fitted on matches from `min_date`; environment and venue
    factors cover every completed match, so older seasons can still be rated.
    """
    fit_matches = completed_matches(spark, t, min_date)
    stats = fit_matches.agg(F.count("*").alias("n"), F.min("match_date").alias("first"),
                            F.max("match_date").alias("last")).first()

    re_pdf = smooth_re(raw_cells(re_observations(spark, t, fit_matches)), min_samples, max_window)
    re_df = spark.createDataFrame(re_pdf).withColumn("version", F.lit(version))
    storage.replace_rows(spark, t.gold_run_expectancy, re_df, re_df, "version")

    rates = match_rates(spark, t, completed_matches(spark, t))
    base = (rates.join(fit_matches.select("match_id"), "match_id")
            .agg((F.sum("runs_1") / F.sum("balls_1")).alias("rate")).first()["rate"])
    env = environment_factors(rates, base, environment_prior_matches)
    venues = venue_factors(rates, env, venue_prior_matches).withColumn("version", F.lit(version))
    env = env.drop("all_innings_rate").withColumn("version", F.lit(version))
    one_version = venues.select("version").limit(1)
    storage.replace_rows(spark, t.gold_environment_factors, env, one_version, "version")
    storage.replace_rows(spark, t.gold_venue_factors, venues, one_version, "version")

    params = {"min_date": min_date, "min_samples": min_samples, "max_window": max_window,
              "venue_prior_matches": venue_prior_matches,
              "environment_prior_matches": environment_prior_matches,
              "baseline_runs_per_over": round(base * 6, 3), "re_fitted_on": "first innings"}
    row = spark.createDataFrame(
        [(version, int(stats["n"]), stats["first"], stats["last"], json.dumps(params), notes)],
        "version string, fitted_matches long, first_match_date date, last_match_date date, "
        "params string, notes string",
    ).withColumn("created_at", F.current_timestamp())
    storage.replace_rows(spark, t.gold_model_versions, row, row, "version")

    start = re_pdf.query("balls_left == @TOTAL_BALLS and wickets == 0")["expected_runs"].iloc[0]
    return {"version": version, "fitted_matches": int(stats["n"]),
            "first_match_date": str(stats["first"]), "last_match_date": str(stats["last"]),
            "expected_first_innings_total": float(start),
            "baseline_runs_per_over": round(base * 6, 2)}
