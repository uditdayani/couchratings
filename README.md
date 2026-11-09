# Cricket Player Ratings

Sofascore-style 0–10 match ratings and form ratings for T20 cricketers, built as a medallion data pipeline (Bronze → Silver → Gold) on Databricks Free Edition with a Streamlit dashboard.

**Status:** Week 1 – setup and connectivity checks.

## Repo layout

```
notebooks/
  01_week1_setup_and_cricsheet_check.py   Databricks notebook: schemas, volume, Cricsheet access, zip download
scripts/
  download_cricsheet.py                    Local fallback: download the zips on your laptop
app/
  streamlit_app.py                         Streamlit check that the SQL warehouse can be queried from outside
.streamlit/
  secrets.toml.example                     Template for the Streamlit secrets (never commit the real file)
requirements.txt                           Packages for the Streamlit app and local scripts
```

## Week 1 checklist

1. **GitHub:** create a public repo called `cricket-ratings` and push these files.
2. **Databricks Free Edition:** sign up, then add the repo as a Git folder (Workspace → Create → Git folder).
3. **Cricsheet access:** open `notebooks/01_week1_setup_and_cricsheet_check.py` in Databricks and run all cells on serverless compute.
   - If the reachability check fails, Free Edition is blocking outbound internet. Either verify your account with LinkedIn (this unlocks outbound internet access), or run `python scripts/download_cricsheet.py` locally and upload the two zips to the `raw_files` volume in Catalog Explorer. Then re-run the notebook.
4. **SQL warehouse from outside:**
   - Create a personal access token (Settings → Developer → Access tokens).
   - Copy your SQL warehouse's server hostname and HTTP path (SQL Warehouses → your warehouse → Connection details).
   - Deploy `app/streamlit_app.py` on Streamlit Community Cloud and paste the values into the app's secrets, using `.streamlit/secrets.toml.example` as the template.

Week 1 is done when the notebook shows both match counts and the Streamlit app shows the same check rows.

## Data and attribution

Ball-by-ball data from Cricsheet (cricsheet.org), used under the Open Data Commons Attribution License.
