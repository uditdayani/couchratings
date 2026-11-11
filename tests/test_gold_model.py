"""Run-expectancy table and venue factors."""
import numpy as np
import pandas as pd
import pytest

from cricket_ratings import gold_model
from cricket_ratings.gold_model import MAX_WICKETS, TOTAL_BALLS
from test_pipeline import built  # noqa: F401  (reuses the fixture that lands + builds Silver)


# ---------------------------------------------------------- smoothing (pure pandas)

def truth(b, w):
    return b * (1.4 - 0.1 * w)   # known answer: linear in balls left, lower with wickets


@pytest.fixture(scope="module")
def smoothed():
    cells = {}
    for w in range(0, 4):                         # dense with plenty of balls left
        for b in range(60, 121):
            cells[(b, w)] = 200
    for b in range(1, 60):                        # wicketless, few balls left...
        if b not in (5, 6, 7, 8):                 # ...with a gap to interpolate
            cells[(b, 0)] = 100
    for w in range(4, 10):                        # dense once wickets have fallen
        for b in range(1, 61):
            cells[(b, w)] = 200
    for b in range(61, 121):                      # 4 down is common at any stage
        cells[(b, 4)] = 200
    for b in range(61, 81):                       # sparse: needs pooling
        cells[(b, 5)] = 20
    raw = pd.DataFrame([{"balls_left": b, "wickets": w, "n": n, "runs_sum": truth(b, w) * n}
                        for (b, w), n in cells.items()])
    re = gold_model.smooth_re(raw, min_samples=50, max_window=12)
    return re.set_index(["balls_left", "wickets"])


def test_table_is_complete_with_terminal_zeros(smoothed):
    assert len(smoothed) == (TOTAL_BALLS + 1) * (MAX_WICKETS + 1)
    assert (smoothed.xs(0, level="balls_left")["expected_runs"] == 0).all()
    assert (smoothed.xs(MAX_WICKETS, level="wickets")["expected_runs"] == 0).all()


def test_table_is_monotone(smoothed):
    grid = smoothed["expected_runs"].unstack("wickets")          # rows: balls left, cols: wickets
    assert (np.diff(grid.values, axis=0) >= -1e-9).all()        # more balls -> never fewer runs
    assert (np.diff(grid.values, axis=1) <= 1e-9).all()         # more wickets -> never more runs


def test_dense_cells_match_the_data(smoothed):
    assert smoothed.loc[(120, 0), "expected_runs"] == pytest.approx(168)
    assert smoothed.loc[(60, 2), "expected_runs"] == pytest.approx(72)
    assert smoothed.loc[(120, 0), "method"] == "observed"


def test_sparse_cells_are_pooled(smoothed):
    cell = smoothed.loc[(70, 5)]
    assert cell["method"] == "pooled" and cell["sample_size"] == 20
    assert cell["expected_runs"] == pytest.approx(truth(70, 5))


def test_gaps_are_filled_without_one_sided_bias(smoothed):
    # Balls 5-8 have no data. The nearest data sits mostly on one side (4 below, 9+ above);
    # a plain average would be biased, the weighted line is not.
    cell = smoothed.loc[(6, 0)]
    assert cell["method"] == "pooled" and cell["sample_size"] == 0
    assert cell["expected_runs"] == pytest.approx(truth(6, 0))
    assert smoothed.loc[(119, 2), "expected_runs"] == pytest.approx(truth(119, 2))


def test_interpolates_when_pooling_cannot_reach():
    raw = pd.DataFrame([{"balls_left": 100, "wickets": 0, "n": 60, "runs_sum": 60 * 140.0}])
    re = gold_model.smooth_re(raw, min_samples=50, max_window=3).set_index(["balls_left", "wickets"])
    cell = re.loc[(50, 0)]
    assert cell["method"] == "interpolated"
    assert cell["expected_runs"] == pytest.approx(70.0)      # halfway between 0 at 0 balls and 140 at 100


def test_unreachable_cells_extend_the_trend_then_hold_flat(smoothed):
    # 9 down with lots of balls left never happens (data stops at 60 balls). The line
    # through the last cells carries on while the window still holds two data cells
    # (up to 71 balls), then the value is held flat - never invented beyond that.
    assert smoothed.loc[(71, 9), "expected_runs"] == pytest.approx(truth(71, 9))
    assert smoothed.loc[(100, 9), "expected_runs"] == smoothed.loc[(120, 9), "expected_runs"]
    assert smoothed.loc[(120, 9), "expected_runs"] == pytest.approx(truth(71, 9))


# ------------------------------------------------------ Spark, on the fixture matches

def test_observations_take_the_first_time_each_state_is_reached(spark, built):  # noqa: F811
    t, _, _ = built
    matches = gold_model.completed_matches(spark, t)
    assert sorted(r.match_id for r in matches.collect()) == ["1000001", "1000003"]  # no-result left out

    obs = gold_model.re_observations(spark, t, matches)
    got = sorted(((r.match_id, r.balls_left, r.wickets, r.runs_to_end) for r in obs.collect()),
                 key=lambda x: (x[0], -x[1], x[2]))
    assert got == [
        ("1000001", 120, 0, 17), ("1000001", 119, 0, 13),   # the wide repeats 119/0: kept once
        ("1000001", 118, 0, 11), ("1000001", 117, 1, 11),   # caught: 1 down
        ("1000001", 116, 1, 8), ("1000001", 115, 1, 7),
        ("1000001", 114, 1, 6), ("1000001", 113, 1, 0),     # run out on the last ball
        ("1000003", 120, 0, 4),
    ]


def test_fit_model_writes_versioned_tables(spark, built):  # noqa: F811
    t, _, _ = built
    for _ in range(2):   # re-fitting the same version replaces it
        summary = gold_model.fit_model(spark, t, "v1", min_samples=1, max_window=0)
    assert summary["fitted_matches"] == 2

    re = spark.table(t.gold_run_expectancy).filter("version = 'v1'")
    assert re.count() == (TOTAL_BALLS + 1) * (MAX_WICKETS + 1)
    cell = lambda b, w: re.filter(f"balls_left = {b} AND wickets = {w}").first()  # noqa: E731
    assert cell(119, 0)["expected_runs"] == 13
    # The raw average at 120 balls is (17 + 4) / 2 = 10.5, below the 13 at 119 balls.
    # More balls can't mean fewer runs, so the monotone step lifts it to 13.
    assert cell(120, 0)["sample_size"] == 2
    assert summary["expected_first_innings_total"] == pytest.approx(13)

    venues = spark.table(t.gold_venue_factors).collect()
    assert [(v.venue, v.matches, v.factor) for v in venues] == [("Test Ground", 2, 1.0)]
    env = spark.table(t.gold_environment_factors).collect()
    assert [(e.competition, e.year, e.matches, e.factor) for e in env] == [("Test League", 2025, 2, 1.0)]
    assert spark.table(t.gold_model_versions).count() == 1


# ------------------------------------------------- environment and venue factors

def test_environment_and_venue_factors(spark):
    # League A scores 1.5 runs a ball in the first innings, league B 1.0. In league A,
    # ground X runs hot and ground Y cold; league B plays only at ground Z.
    rows = []
    for i in range(10):
        hot = i < 5
        rows.append((f"a{i}", "X" if hot else "Y", "League A", 2025, 180, 120, 440 if hot else 360, 240))
        rows.append((f"b{i}", "Z", "League B", 2025, 120, 120, 240, 240))
    rates = spark.createDataFrame(rows, "match_id string, venue string, competition string, year int, "
                                        "runs_1 long, balls_1 long, runs_all long, balls_all long")
    env = gold_model.environment_factors(rates, baseline_rate=1.25, prior_matches=5)
    e = {r.competition: r for r in env.collect()}
    assert e["League A"].raw_factor == pytest.approx(1.2)                     # 1.5 / 1.25
    assert e["League A"].factor == pytest.approx(1 + 10 / 15 * 0.2, abs=1e-4)  # shrunk: 10 matches
    assert e["League B"].factor == pytest.approx(1 - 10 / 15 * 0.2, abs=1e-4)

    v = {r.venue: r for r in gold_model.venue_factors(rates, env, prior_matches=10).collect()}
    assert v["X"].raw_factor == pytest.approx(1.1)        # hot within League A
    assert v["Y"].raw_factor == pytest.approx(0.9)
    assert v["Z"].factor == pytest.approx(1.0)            # low-scoring league, but a neutral ground
    assert v["X"].factor == pytest.approx(1 + 5 / 15 * 0.1, abs=1e-4)
