# Cricket Player Ratings

Sofascore-style 0–10 match ratings and form ratings for T20 cricketers, built as a medallion data pipeline (Bronze → Silver → Gold) on Databricks Free Edition with a Streamlit dashboard.

**Status:** Week 2 – Bronze and Silver layers with data quality checks.

## Repo layout

```
src/cricket_ratings/        Pipeline code shared by the notebooks and the tests
  config.py                 Table names in one place
  schema.py                 Spark schema for a Cricsheet JSON match file
  bronze.py                 Unzip and land raw files, once per version
  silver.py                 Parse into matches, innings, deliveries, players
  quality.py                Data quality checks that stop the pipeline on bad data
  storage.py                All Delta table writes (MERGE / append / overwrite)
notebooks/
  01_week1_setup_and_cricsheet_check.py
  02_bronze_land.py         Week 2, step 1
  03_silver_build.py        Week 2, step 2
  04_silver_checks.py       Week 2, step 3
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
| Ops | `ops.ingest_log` | file processed at each stage (landed, parsed, quarantined) |
| Ops | `ops.dq_results` | data quality check result per run |

Design choices:
- **Idempotent:** re-running any step adds nothing. Bronze is keyed on file name + SHA-256 hash; Silver replaces a match's rows rather than appending.
- **Incremental:** Silver only processes files (or new versions of files) it hasn't seen.
- **Quality gate:** error-level checks (duplicates, run arithmetic, unresolved player IDs, target reconciliation) fail the run before Gold is built.
- **Legal balls:** `legal_ball_no` counts deliveries that aren't wides or no-balls, which is what run expectancy needs.

## Running week 2 on Databricks

Pull the latest code into your Git folder, then run on serverless compute, in order:

1. `notebooks/02_bronze_land.py`
2. `notebooks/03_silver_build.py`
3. `notebooks/04_silver_checks.py`

## Tests

```bash
pip install -r requirements-dev.txt   # needs Java 17+
pytest -q tests
```

The tests cover legal-ball counting with wides and no-balls, wickets and substitute fielders, no-result and super-over matches, quarantining bad files, idempotent re-runs, revised files, and the quality checks catching broken rows. Delta-only writes (in `storage.py`) are swapped for plain-Spark equivalents in tests.

## Data and attribution

Ball-by-ball data from Cricsheet (cricsheet.org), used under the Open Data Commons Attribution License.
