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
from conftest import rewrite_table
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
    gold_model.fit_model(spark, t, "v1", min_samples=1)
    for _ in range(2):                                                # re-running replaces the version
        summary = gold_ratings.run_ratings(spark, t, "v1")
    assert summary["balls_valued"] == 13 + 2                          # match 1000001 + 1000003 (no super overs)
    assert summary["player_matches"] == 12                            # 6 players x 2 rated matches
    with pytest.raises(ValueError, match="v9"):
        gold_ratings.run_ratings(spark, t, "v9")


def test_par_rates_and_bowler_par(spark, built):  # noqa: F811
    """Fixture matches 1000001 and 1000003, both innings. Over 1 with 0-2 down: 11 legal balls,
    24 runs charged to bowlers (wides and no-balls in, the leg-bye out), 2 bowler wickets
    (a catch and a bowled). Over 2: 2 legal balls, 6 runs, and a run out (not the bowler's)."""
    t, _, _ = built
    gold_model.fit_model(spark, t, "v1", min_samples=1)
    rates = {(r.over_no, r.wickets_band): r for r in
             spark.table(t.gold_par_rates).filter("version = 'v1'").collect()}
    assert set(rates) == {(1, "0-2"), (2, "0-2")}
    assert (rates[(1, "0-2")].legal_balls, rates[(1, "0-2")].runs_per_ball) == (11, pytest.approx(24 / 11, abs=1e-4))
    assert rates[(1, "0-2")].wickets_per_ball == pytest.approx(2 / 11, abs=1e-5)
    assert (rates[(2, "0-2")].runs_per_ball, rates[(2, "0-2")].wickets_per_ball) == (3.0, 0.0)

    gold_ratings.run_ratings(spark, t, "v1")
    scale = spark.table(t.gold_environment_factors).first().factor           # venue factor is 1.0
    b1 = spark.table(t.gold_player_match_impact).filter(
        f"version = 'v1' AND match_id = '1000001' AND player_id = '{B1}'").first()
    # B1 bowled over 1: 6 legal balls (the wide and no-ball earn no par).
    assert b1.par_runs_conceded == pytest.approx(6 * 24 / 11 * scale, abs=0.01)
    assert b1.par_wickets == pytest.approx(6 * 2 / 11, abs=0.001)
    a1 = spark.table(t.gold_player_match_impact).filter(
        f"version = 'v1' AND match_id = '1000001' AND player_id = '{A1}'").first()
    assert a1.par_runs_conceded == pytest.approx(3 * 24 / 11 * scale, abs=0.01)  # 3 balls in innings 2


def _starts(spark, t, overs_limit, balls_per_over, target_overs):
    """First-ball balls-left for each innings of 1000001 after changing its limits."""
    inn = spark.table(t.silver_innings)
    rewrite_table(spark, t.silver_innings, inn.withColumn(
        "target_overs", F.when((F.col("match_id") == "1000001") & (F.col("innings_no") == 2),
                               F.lit(target_overs)).otherwise(F.col("target_overs"))))
    m = spark.table(t.silver_matches)
    mine = F.col("match_id") == "1000001"
    rewrite_table(spark, t.silver_matches, m
                  .withColumn("overs_limit", F.when(mine, F.lit(overs_limit)).otherwise(F.col("overs_limit")))
                  .withColumn("balls_per_over", F.when(mine, F.lit(balls_per_over)).otherwise(F.col("balls_per_over"))))
    env, venues = factors(spark)
    bv = gold_ratings.ball_values(spark.table(t.silver_deliveries), gold_ratings.rated_matches(spark, t),
                                  linear_re(spark), env, venues).filter("match_id = '1000001' AND seq = 1")
    return {r.innings_no: r.b_before for r in bv.collect()}


def test_rain_reduced_chase_does_not_shorten_a_completed_first_innings(spark, built):  # noqa: F811
    """Regression: a full first innings (8 balls of an 8-ball limit), then rain cuts the
    chase to 1 over (6 balls). Innings 1 must keep its full 8; the old code used 6 for both,
    treating the end of innings 1 as "no balls left" and every run there as a free bonus."""
    t, _, _ = built
    assert _starts(spark, t, overs_limit=2, balls_per_over=4, target_overs=1.0) == {1: 8, 2: 6}


def test_first_innings_cut_short_uses_the_reduced_length(spark, built):  # noqa: F811
    """A 12-ball limit, innings 1 stops after 8 balls with 2 down (cut short), and the chase
    target is set over 1.4 overs (10 balls): both innings are treated as 10 balls long."""
    t, _, _ = built
    assert _starts(spark, t, overs_limit=2, balls_per_over=6, target_overs=1.4) == {1: 10, 2: 10}


def test_batting_stages(spark, built):  # noqa: F811
    t, _, _ = built
    bv, _ = impact(spark, t)
    st = {r.player_id: r.batting_stage for r in
          gold_ratings.batting_stages(bv).filter("match_id = '1000001'").collect()}
    # Openers are at the crease for the first ball (striker or non-striker); A3 came in after
    # A2's dismissal with 117 balls left, B3 at the third ball of the chase: both "top order".
    assert st == {A1: "opener", A2: "opener", A3: "top order", B1: "opener", B2: "opener", B3: "top order"}


def test_byes_are_charged_to_the_keeper(spark):
    wk = "array<struct<kind:string,player_out_id:string,fielder_ids:array<string>>>"
    bv = spark.createDataFrame([
        ("m1", "Y", 0, 1, [("stumped", "y1", ["k1"])]),    # k1 has a career stumping
        ("m2", "Y", 4, 0, []),                               # Y bat in m2: 4 byes off X's keeper
        ("m2", "X", 2, 0, []),                               # X bat in m2: Y have no stumper
    ], f"match_id string, batting_team string, extras_byes int, dismissals int, wickets {wk}")
    xi = spark.createDataFrame([("m2", "k1", "X"), ("m2", "x2", "X"), ("m2", "y1", "Y")],
                               "match_id string, player_id string, team string")
    rows = [(r.match_id, r.player_id, r.fielding, r.byes_conceded)
            for r in gold_ratings.keeper_byes(bv, xi).collect()]
    assert rows == [("m2", "k1", -4.0, 4)]
