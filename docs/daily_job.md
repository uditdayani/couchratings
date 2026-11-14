# Daily job: set-up guide

One Databricks job runs the pipeline every morning: download new matches, update Bronze and
Silver, check data quality, rebuild ratings and form, then validate. If any step fails, the
later steps don't run and you get an email.

## Tasks

| # | Task name | Notebook | Parameters | Depends on |
| --- | --- | --- | --- | --- |
| 1 | `land_bronze` | `notebooks/02_bronze_land` | `catalog` = `workspace`, `download` = `recent`, `sources` = `recent_male` | – |
| 2 | `build_silver` | `notebooks/03_silver_build` | `catalog` = `workspace` | `land_bronze` |
| 3 | `silver_checks` | `notebooks/04_silver_checks` | `catalog` = `workspace` | `build_silver` |
| 4 | `ratings` | `notebooks/06_gold_ratings` | `catalog` = `workspace`, `model_version` = `v2` | `silver_checks` |
| 5 | `scale_and_form` | `notebooks/07_gold_scale_form` | `catalog` = `workspace`, `model_version` = `v2` | `ratings` |
| 6 | `validation` | `notebooks/08_validation` | `catalog` = `workspace`, `model_version` = `v2` | `scale_and_form` |

`05_gold_model` is **not** in the daily job: the run-expectancy table is only re-fitted when
the model changes (as a new version, e.g. `v3`).

## Creating it in Databricks

1. In the left sidebar, open **Jobs & Pipelines** → **Create** → **Job**. Name it `cricket-ratings-daily`.
2. Set up the first task:
   - **Task name:** `land_bronze`
   - **Type:** Notebook
   - **Source:** Workspace
   - **Path:** browse to your Git folder → `notebooks/02_bronze_land`
   - **Compute:** Serverless
   - **Parameters:** add each key and value from the table above
3. Click **+ Add task** → **Notebook** for each remaining task, in order. Set **Depends on** to the
   previous task, and add the parameters from the table.
4. In the job's right-hand panel:
   - **Schedules & Triggers** → **Add trigger** → **Scheduled**, daily at 07:30, time zone Europe/London.
   - **Job notifications** → **Edit** → add your email for **Failure**.
5. Click **Run now** once to test. Each task turns green as it finishes. The first run picks up
   every men's IPL or T20I match Cricsheet added in the last 7 days.

## Good to know

- **The job runs the code in your Git folder.** After pushing changes to GitHub, open the
  Git folder in Databricks and click **Pull**.
- **What gets kept:** the 7-day zip includes every format and league. Silver keeps men's T20
  internationals and the IPL, and logs everything else in `ops.ingest_log` as
  `out of scope: <event>`. To rate another league, add it to `IN_SCOPE_CLUB_EVENTS` in
  `src/cricket_ratings/silver.py` and backfill its zip once.
- **Dashboard freshness:** the app caches data for 12 hours. To show a run's results straight
  away, use **⋮ → Clear cache** in the app.
- **Free Edition:** the tasks run one at a time, well within the 5-concurrent-task limit.
  Log in to Databricks at least monthly so the account counts as active.
