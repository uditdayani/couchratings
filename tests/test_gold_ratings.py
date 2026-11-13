"""Rating engine: ball values and player credits, checked against hand-worked numbers.

The test RE table is RE(b, w) = b x (1 - 0.1 w), so using up a ball costs 1.0 run with
no wickets down and 0.9 with one down, and a wicket with 117 balls left costs 11.7.
Match 1000001 (see factories.match_a), first innings, Beta bowling:

 seq  ball                              striker  bowler   other
  1   A1 4 runs           (120 -> 119)  A1 +3    B1 -3
  2   wide                              -        B1 -1
  3   A1 1 run            (119 -> 118)  A1  0    B1  0
  4   A2 caught by B2     (118 -> 117)  A2 -1-11.7   B1 +1 + 0.8 x 11.7   B2 fielding +0.2 x 11.7
  5   no-ball, A3 2 runs  (117, 1 down) A3 +2    B1 -3
  6   A3 dot              (117 -> 116)  A3 -0.9  B1 +0.9
  7   leg-bye             (116 -> 115)  A3 -0.9  B1 +0.9 (leg-bye not charged)
  8   A1 1 run            (115 -> 114)  A1 +0.1  B1 -0.1
  9   A1 six (B2 bowling) (114 -> 113)  A1 +5.1  B2 -5.1
 10   A3 run out by B3    (113 -> 112)  A3 -0.9-11.2   B2 +0.9   B3 fielding +11.2
"""
import pytest
from pyspark.sql import functions as F

from cricket_ratings import gold_model, gold_ratings
from cricket_ratings.gold_model import MAX_WICKETS, TOTAL_BALLS
from test_pipeline import built  # noqa: F401


def linear_re(spark):
    rows = [(b, w, 0.0 if w == MAX_WICKETS else b * (1 - 0.1 * w))
            for b in range(TOTAL_BALLS + 1) for w in range(MAX_WICKETS + 1)]
    return spark.createDataFrame(rows, "balls_left int, wickets int, expected_runs double")


def factors(spark, env_factor=1.0):
    env = spark.createDataFrame([("Test League", 2025, env_factor)], "competition string, year int, factor double")
    venues = spark.createDataFrame([("Test Ground", 1.0)], "venue string, factor double")
    return env, venues


def impact(spark, t, env_factor=1.0):
    env, venues = factors(spark, env_factor)
    bv = gold_ratings.ball_values(spark.table(t.silver_deliveries), gold_ratings.rated_matches(spark, t),
                                  linear_re(spark), env, venues)
    rows = gold_ratings.player_impact(bv, spark.table(t.silver_match_players)) \
        .filter("match_id = '1000001'").collect()
    return bv, {r.player_id: r for r in rows}


A1, A2, A3, B1, B2, B3 = "a0000001", "a0000002", "a0000003", "b0000001", "b0000002", "b0000003"


def test_batting_credits(spark, built):  # noqa: F811
    t, _, _ = built
    _, p = impact(spark, t)
    assert p[A1].batting == pytest.approx(3 + 0 + 0.1 + 5.1)        # 8.2
    assert p[A2].batting == pytest.approx(-1 - 11.7)                 # out first ball faced
    assert p[A3].batting == pytest.approx(2 - 0.9 - 0.9 - 0.9 - 11.2)


def test_bowling_credits(spark, built):  # noqa: F811
    t, _, _ = built
    _, p = impact(spark, t)
    assert p[B1].bowling == pytest.approx(-3 - 1 + 0 + (1 + 0.8 * 11.7) - 3 + 0.9 + 0.9 - 0.1)
    assert p[B2].bowling == pytest.approx(-5.1 + 0.9)                # no bowler credit for a run out


def test_fielding_credits_include_substitutes(spark, built):  # noqa: F811
    t, _, _ = built
    _, p = impact(spark, t)
    assert p[B2].fielding == pytest.approx(0.2 * 11.7)               # the catch
    assert p[B3].fielding == pytest.approx(11.2)                     # run out, as a substitute


def test_counting_stats(spark, built):  # noqa: F811
    t, _, _ = built
    _, p = impact(spark, t)
    assert (p[A1].runs, p[A1].balls_faced) == (12, 4)                # the wide isn't a ball faced
    assert (p[B1].balls_bowled, p[B1].runs_conceded, p[B1].wickets) == (6, 10, 1)  # leg-bye not conceded
    assert (p[B2].catches, p[B3].run_outs, p[B2].wickets) == (1, 1, 0)
    assert (p[A2].dismissed, p[A3].dismissed, p[A1].dismissed) == (1, 1, 0)
    assert len(p) == 6                                                # every listed player has a row
    assert p[A1].total_impact == pytest.approx(p[A1].batting + p[A1].bowling + p[A1].fielding, abs=1e-3)


def test_environment_factor_scales_expected_runs(spark, built):  # noqa: F811
    t, _, _ = built
    _, p = impact(spark, t, env_factor=2.0)
    assert p[A2].batting == pytest.approx(-2 - 23.4)                 # ball and wicket cost both double


def test_only_rated_balls_are_valued(spark, built):  # noqa: F811
    t, _, _ = built
    bv, _ = impact(spark, t)
    innings = {(r.match_id, r.innings_no) for r in bv.select("match_id", "innings_no").distinct().collect()}
    # 1000002 (no result) is not rated; 1000003's super overs (innings 3 and 4) are left out.
    assert innings == {("1000001", 1), ("1000001", 2), ("1000003", 1), ("1000003", 2)}
    first = bv.filter("match_id = '1000001' AND innings_no = 1 AND seq = 1").first()
    assert (first.b_before, first.b_after, first.runs_added) == (120, 119, pytest.approx(3.0))


def test_allotted_balls_for_shortened_matches(spark):
    df = spark.createDataFrame([(None, 20, 6), (20.0, 20, 6), (10.0, 20, 6), (17.4, 20, 6)],
                               "target_overs double, overs_limit int, balls_per_over int")
    got = [r.a for r in df.select(gold_ratings.allotted_balls(
        F.col("target_overs"), F.col("overs_limit"), F.col("balls_per_over")).alias("a")).collect()]
    assert got == [120, 120, 60, 106]                                 # 17.4 overs = 17 x 6 + 4


def test_t20i_split_by_full_members(spark):
    df = spark.createDataFrame(
        [("international", None, "India", "Australia"), ("international", None, "India", "Nepal"),
         ("club", "Indian Premier League", "X", "Y")],
        "team_type string, event_name string, team_1 string, team_2 string")
    assert [r.c for r in df.select(gold_model.competition_col().alias("c")).collect()] == [
        "T20I full members", "T20I other", "Indian Premier League"]


def test_run_ratings_end_to_end(spark, built):  # noqa: F811
    t, _, _ = built
    gold_model.fit_model(spark, t, "v1", min_samples=1, max_window=0)
    for _ in range(2):                                                # re-running replaces the version
        summary = gold_ratings.run_ratings(spark, t, "v1")
    assert summary["balls_valued"] == 13 + 2                          # match 1000001 + 1000003 (no super overs)
    assert summary["player_matches"] == 12                            # 6 players x 2 rated matches
    with pytest.raises(ValueError, match="v9"):
        gold_ratings.run_ratings(spark, t, "v9")
