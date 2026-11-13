# Cricket Player Ratings

Sofascore-style 0–10 match ratings and form ratings for T20 cricketers, built as a medallion data pipeline (Bronze → Silver → Gold) on Databricks Free Edition with a Streamlit dashboard.

**Status:** Week 4 – rating engine: every ball valued, every player credited (impact in runs).

## Repo layout

```
src/cricket_ratings/        Pipeline code shared by the notebooks and the tests
  config.py                 Table names in one place
  schema.py                 Spark schema for a Cricsheet JSON match file
  bronze.py                 Unzip and land raw files, once per version
  silver.py                 Parse into matches, innings, deliveries, players
  quality.py                Data quality checks that stop the pipeline on bad data
  gold_model.py             Run-expectancy table, environment and venue factors, per model version
  gold_ratings.py           Rating engine: ball values and player credits
  storage.py                All Delta table writes (MERGE / append / overwrite)
notebooks/
  01_week1_setup_and_cricsheet_check.py
  02_bronze_land.py         Week 2, step 1
  03_silver_build.py        Week 2, step 2
  04_silver_checks.py       Week 2, step 3
  05_gold_model.py          Week 3: fit the model tables and sanity-check them
  06_gold_ratings.py        Week 4: value every ball and credit players
tests/                      pytest suite on small hand-built matches
app/streamlit_app.py        Dashboard (week 1: connection check)
scripts/download_cricsheet.py  Local fallback download
```

## Pipeline so far

| Layer | Table | One row per |
| --- | --- | --- |
| Bronze | `bronze.match_files` | version of a raw Cricsheet file (append-only) |
| Silver | `silver.matches` | match |
| Silver | `silver.innings` | innings, with totals computed from deliveries |
| Silver | `silver.deliveries` | ball, including wides and no-balls |
| Silver | `silver.match_players` | player in a match |
| Silver | `silver.players` | player (Cricsheet Register ID) |
| Gold | `gold.run_expectancy` | (model version, balls left, wickets down): expected runs to the end of the innings |
| Gold | `gold.environment_factors` | (model version, competition, year): how high-scoring that competition-season was |
| Gold | `gold.venue_factors` | (model version, venue): how high-scoring the ground is within its competition |
| Gold | `gold.ball_values` | (model version, ball): state before/after, ball cost, wicket cost, runs added |
| Gold | `gold.player_match_impact` | (model version, match, player): batting, bowling and fielding impact in runs, plus counting stats |
| Gold | `gold.model_versions` | model version: when and how it was fitted |
| Ops | `ops.ingest_log` | file processed at each stage (landed, parsed, quarantined) |
| Ops | `ops.dq_results` | data quality check result per run |

Design choices:
- **Idempotent:** re-running any step adds nothing. Bronze is keyed on file name + SHA-256 hash; Silver replaces a match's rows rather than appending.
- **Incremental:** Silver only processes files (or new versions of files) it hasn't seen.
- **Quality gate:** error-level checks (duplicates, run arithmetic, unresolved player IDs, target reconciliation) fail the run before Gold is built.
- **Legal balls:** `legal_ball_no` counts deliveries that aren't wides or no-balls, which is what run expectancy needs.

## The run-expectancy model

`RE(b, w)` is the average number of runs a side scores from a point in its innings to the end, with `b` legal balls left and `w` wickets down. Week 4 values each ball by how much it changes this number.

- **Fitted on first innings only**, from full-length completed matches. A chase stops when the target is reached, which would cut "runs to the end" short. V1 uses the same table for both innings.
- **Each innings counts once per state**, the first time it reaches it, so a wide doesn't double-count a state.
- **Sparse cells are pooled** with neighbouring balls-left cells by fitting a weighted straight line, so neighbours on one side only (a gap, or the 120-ball edge) don't bias the estimate. Cells with no nearby data are interpolated, then the table is made monotone: more balls never means fewer runs, and more wickets never means more.
- **Fitted on 2019 onwards** (`v2`) for modern batting patterns.
- **Scoring environment:** the IPL now averages 190+ in the first innings while T20Is (mostly associate nations since 2019) average about 150. One table alone would overrate every IPL batter, so expected runs = `RE(b, w) × environment factor × venue factor`. The environment factor is a competition-year's first-innings runs per ball ÷ the fitting set's, and covers every season, so older matches are rated against their own era.
- **Venue factor** is measured against what each match's competition and year predict, so a ground isn't "high-scoring" just for hosting the IPL. Both factors are shrunk towards 1.0 when they rest on few matches: `1 + m / (m + k) × (raw − 1)`, with k = 5 (environment) and 10 (venue).

## The rating engine

Every ball is valued by how much it changed the batting side's expected runs: `runs added = runs + E(after) − E(before)`, split into the **ball cost** (using up a legal ball) and the **wicket cost** (the drop when a batter is out).

| Player | Credit |
| --- | --- |
| Striker | runs off the bat + ball cost |
| Batter who is out | wicket cost |
| Bowler | −(runs off the bat + wides + no-balls) − ball cost + bowler share of the wicket |
| Fielders | fielder share of the wicket, split between the fielders named |

Wicket shares (starting values): bowled, lbw, hit wicket, caught and bowled 100% bowler; caught 80% bowler / 20% catcher; stumped 70% / 30% keeper; run out 100% to the fielders. Byes and leg-byes aren't charged to the bowler. Shortened matches use the reduced overs. Super overs and no-results aren't rated. T20Is are split into full-member matches and those involving associates, which score very differently.

## Running week 2 on Databricks

Pull the latest code into your Git folder, then run on serverless compute, in order:

1. `notebooks/02_bronze_land.py`
2. `notebooks/03_silver_build.py`
3. `notebooks/04_silver_checks.py`
4. `notebooks/05_gold_model.py` (week 3)
5. `notebooks/06_gold_ratings.py` (week 4)

## Tests

```bash
pip install -r requirements-dev.txt   # needs Java 17+
pytest -q tests
```

The tests cover the rating engine against hand-worked credits for a full innings (wides, no-balls, leg-byes, a catch, a substitute's run out, scaling), the run-expectancy smoothing against a table with a known answer (pooling, gaps, edges, monotonicity), the first-time-in-state observations, versioned model writes, legal-ball counting with wides and no-balls, wickets and substitute fielders, no-result and super-over matches, quarantining bad files, idempotent re-runs, revised files, and the quality checks catching broken rows. Delta-only writes (in `storage.py`) are swapped for plain-Spark equivalents in tests.

## Data and attribution

Ball-by-ball data from Cricsheet (cricsheet.org), used under the Open Data Commons Attribution License.
