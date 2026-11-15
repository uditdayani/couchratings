"""Run-expectancy table and venue factors."""
import numpy as np
import pandas as pd
import pytest

from cricket_ratings import gold_model
from cricket_ratings.gold_model import MAX_WICKETS, TOTAL_BALLS
from test_pipeline import built  # noqa: F401  (reuses the fixture that lands + builds Silver)


# ---------------------------------------------------------- smoothing (pure pandas)

Z0 = [220, 190, 165, 140, 115, 90, 68, 48, 30, 15]          # runs a side could still add, by wickets down


def truth(b, w):
    """A known, Duckworth-Lewis-shaped answer: rises with balls left and levels off."""
    return Z0[w] * (1 - np.exp(-(0.015 + 0.002 * w) * b))


def make_raw(cells, outliers=None):
    outliers = outliers or {}
    return pd.DataFrame([{"balls_left": b, "wickets": w, "n": n,
                          "runs_sum": (truth(b, w) + outliers.get((b, w), 0.0)) * n}
                         for (b, w), n in cells.items()])


@pytest.fixture(scope="module")
def smoothed():
    cells = {}
    for w in range(0, 4):                         # few wickets down: data from 60 to 120 balls left
        for b in range(60, 121):
            cells[(b, w)] = 200
    for b in range(1, 60):                        # wicketless near the end, with a gap at 5-8
        if b not in (5, 6, 7, 8):
            cells[(b, 0)] = 100
    for w in range(4, 10):                        # many down: data only late in the innings
        for b in range(1, 61):
            cells[(b, w)] = 200
    for b in range(61, 111):                      # like real data: 4 down reaches further back...
        cells[(b, 4)] = 200
    for b in range(61, 91):                       # ...than 5 down, whose data stops at 90 balls left
        cells[(b, 5)] = 200
    cells[(70, 6)] = 10                           # a thin cell with an unlucky average
    raw = make_raw(cells, outliers={(70, 6): 20.0})
    return gold_model.smooth_re(raw, min_samples=50).set_index(["balls_left", "wickets"])


def test_fit_curve_recovers_a_known_curve():
    b = np.arange(1, 121)
    z0, k = gold_model.fit_curve(b, 150 * (1 - np.exp(-0.02 * b)), np.ones(len(b)))
    assert z0 == pytest.approx(150, rel=0.01) and k == pytest.approx(0.02, rel=0.01)


def test_table_is_complete_with_terminal_zeros(smoothed):
    assert len(smoothed) == (TOTAL_BALLS + 1) * (MAX_WICKETS + 1)
    assert (smoothed.xs(0, level="balls_left")["expected_runs"] == 0).all()
    assert (smoothed.xs(MAX_WICKETS, level="wickets")["expected_runs"] == 0).all()


def test_table_is_monotone(smoothed):
    grid = smoothed["expected_runs"].unstack("wickets")          # rows: balls left, cols: wickets
    assert (np.diff(grid.values, axis=0) >= -1e-9).all()        # more balls -> never fewer runs
    assert (np.diff(grid.values, axis=1) <= 1e-9).all()         # more wickets -> never more runs


def test_well_observed_cells_match_the_data(smoothed):
    for b, w in [(120, 0), (90, 2), (30, 6), (6, 9)]:
        assert smoothed.loc[(b, w), "expected_runs"] == pytest.approx(truth(b, w), abs=0.5)
    assert smoothed.loc[(120, 0), "method"] == "observed"


def test_thin_cells_lean_on_the_curve(smoothed):
    # 10 innings averaging 20 runs above the truth: (10 x +20) / (10 + 50) = +3.3, not +20.
    cell = smoothed.loc[(70, 6)]
    assert cell["method"] == "blended" and cell["sample_size"] == 10
    assert cell["expected_runs"] == pytest.approx(truth(70, 6) + 20 * 10 / 60, abs=0.6)


def test_empty_cells_follow_the_curve(smoothed):
    for b, w in [(6, 0), (100, 5), (120, 9)]:                   # a gap, beyond the data, unreachable
        assert smoothed.loc[(b, w), "method"] == "curve"
        assert smoothed.loc[(b, w), "expected_runs"] == pytest.approx(truth(b, w), abs=1.0)


def test_no_cliff_where_one_level_runs_out_of_data(smoothed):
    """Regression (V1, AR Patel 27 Apr 2026): 4 down has data up to 110 balls left, 5 down only
    up to 90. V1 held 5-down flat beyond its data, so the 4->5 wicket at 104 balls cost 29 runs
    before scaling. The curve keeps it at the true gap."""
    gap = smoothed.loc[(104, 4), "expected_runs"] - smoothed.loc[(104, 5), "expected_runs"]
    assert gap == pytest.approx(truth(104, 4) - truth(104, 5), abs=1.5)
    assert gap < 25


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
        summary = gold_model.fit_model(spark, t, "v1", min_samples=1)
    assert summary["fitted_matches"] == 2

    re = spark.table(t.gold_run_expectancy).filter("version = 'v1'")
    assert re.count() == (TOTAL_BALLS + 1) * (MAX_WICKETS + 1)
    cell = lambda b, w: re.filter(f"balls_left = {b} AND wickets = {w}").first()  # noqa: E731
    assert cell(120, 0)["sample_size"] == 2                    # both first innings start here
    start = summary["expected_first_innings_total"]
    assert start == cell(120, 0)["expected_runs"] and start > 0

    venues = spark.table(t.gold_venue_factors).collect()
    assert [(v.venue, v.matches, v.factor) for v in venues] == [("Test Ground", 2, 1.0)]
    env = spark.table(t.gold_environment_factors).collect()
    # Average first-innings total 10.5 vs RE(120, 0): with 2 matches the raw factor is pulled
    # towards the competition level, itself pulled towards 1.0 (prior 5 matches).
    raw = 10.5 / start
    level = 1 + 2 / 7 * (raw - 1)
    assert [(e.competition, e.year, e.matches) for e in env] == [("Test League", 2025, 2)]
    assert env[0].factor == pytest.approx(level + 2 / 7 * (raw - level), abs=1e-3)
    assert spark.table(t.gold_model_versions).count() == 1


# ------------------------------------------------- environment and venue factors

def test_environment_and_venue_factors(spark):
    # League A averages 180 in the first innings in 2024 (10 matches) and 240 in 2025 (only 2).
    # League B averages 120. Baseline (RE at 120 balls, 0 down) is 150.
    # In League A 2024, ground X runs hot and ground Y cold; League B plays only at ground Z.
    rows = []
    for i in range(10):
        hot = i < 5
        rows.append((f"a{i}", "X" if hot else "Y", "League A", 2024, 180, 120, 440 if hot else 360, 240))
        rows.append((f"b{i}", "Z", "League B", 2024, 120, 120, 240, 240))
    rows += [(f"c{i}", "W", "League A", 2025, 240, 120, 480, 240) for i in range(2)]
    rates = spark.createDataFrame(rows, "match_id string, venue string, competition string, year int, "
                                        "runs_1 long, balls_1 long, runs_all long, balls_all long")
    env = {(r.competition, r.year): r for r in
           gold_model.environment_factors(rates, baseline_total=150, prior_matches=5).collect()}

    level_a = 1 + 12 / 17 * (190 / 150 - 1)            # A's all-years level: 12 matches averaging 190
    assert env[("League A", 2024)].raw_factor == pytest.approx(1.2)
    assert env[("League A", 2024)].factor == pytest.approx(level_a + 10 / 15 * (1.2 - level_a), abs=1e-4)
    # The 2-match season is pulled towards League A's level, not towards 1.0.
    a25 = env[("League A", 2025)]
    assert a25.factor == pytest.approx(level_a + 2 / 7 * (1.6 - level_a), abs=1e-4)
    assert a25.factor > level_a > 1.2 - 0.1
    assert env[("League B", 2024)].factor < 1

    v = {r.venue: r for r in gold_model.venue_factors(rates, gold_model.environment_factors(
        rates, baseline_total=150, prior_matches=5), prior_matches=10).collect()}
    assert v["X"].raw_factor == pytest.approx(1.1)        # hot within League A 2024
    assert v["Y"].raw_factor == pytest.approx(0.9)
    assert v["Z"].factor == pytest.approx(1.0)            # low-scoring league, but a neutral ground
    assert v["X"].factor == pytest.approx(1 + 5 / 15 * 0.1, abs=1e-4)
