"""All data the dashboard shows, loaded once and cached.

The Gold tables are small (a few hundred thousand rows), so each is read whole from the
Databricks SQL warehouse and cached for 12 hours. Clicks are then instant, and the Free
Edition warehouse is barely used.

Demo mode: set the environment variable CRICKET_RATINGS_DATA_DIR to a folder of
<table>.parquet files and the app reads those instead (used by the tests and for running
the dashboard without Databricks).
"""
import os

import pandas as pd
import streamlit as st

CACHE_SECONDS = 12 * 3600
OPTIONAL = {"validation"}   # the app still works if these tables don't exist yet


class DataError(Exception):
    """Raised when the dashboard can't load its data; the message says why."""


def _secrets(section: str) -> dict:
    try:
        return dict(st.secrets.get(section, {}))
    except Exception:            # no secrets file at all (e.g. demo mode)
        return {}


def settings() -> dict:
    db, app = _secrets("databricks"), _secrets("app")
    return {"catalog": db.get("catalog", "workspace"),
            "version": app.get("model_version", "v2"),
            "demo_dir": os.environ.get("CRICKET_RATINGS_DATA_DIR")}


def queries(c: str) -> dict:
    """SQL for each dataset. `c` is the catalog; :version is bound as a parameter."""
    return {
        "ratings": f"""
            SELECT r.match_id, r.match_date, r.competition, r.team, r.player_id, p.name,
                   r.involved, r.rating, r.percentile, r.total_impact, r.batting, r.bowling,
                   r.fielding, r.runs, r.balls_faced, r.dismissed, r.balls_bowled,
                   r.runs_conceded, r.wickets, r.catches, r.stumpings, r.run_outs
            FROM {c}.gold.player_match_ratings r
            LEFT JOIN {c}.silver.players p ON p.player_id = r.player_id
            WHERE r.version = :version""",
        "matches": f"""
            SELECT m.match_id, m.match_date, m.season, m.event_name, m.match_number, m.venue,
                   m.city, m.team_1, m.team_2, m.toss_winner, m.toss_decision, m.result,
                   m.winner, m.win_by_runs, m.win_by_wickets, m.method, m.super_over_winner,
                   m.player_of_match_ids,
                   i1.batting_team AS inn1_team, i1.total_runs AS inn1_runs,
                   i1.wickets AS inn1_wickets, i1.legal_balls AS inn1_balls,
                   i2.batting_team AS inn2_team, i2.total_runs AS inn2_runs,
                   i2.wickets AS inn2_wickets, i2.legal_balls AS inn2_balls
            FROM {c}.silver.matches m
            LEFT JOIN {c}.silver.innings i1 ON i1.match_id = m.match_id AND i1.innings_no = 1
            LEFT JOIN {c}.silver.innings i2 ON i2.match_id = m.match_id AND i2.innings_no = 2
            WHERE m.match_id IN (SELECT DISTINCT match_id FROM {c}.gold.player_match_ratings
                                 WHERE version = :version)""",
        "form": f"""
            SELECT scope, player_id, name, form_rating, matches_counted, last_match_date,
                   active, leaderboard_rank
            FROM {c}.gold.player_form WHERE version = :version""",
        "meta": f"""
            SELECT v.version, v.created_at, v.fitted_matches, v.first_match_date,
                   v.last_match_date AS fitted_to, v.params, v.notes,
                   (SELECT max(match_date) FROM {c}.silver.matches) AS latest_match,
                   (SELECT max(logged_at) FROM {c}.ops.ingest_log) AS last_ingest
            FROM {c}.gold.model_versions v WHERE v.version = :version""",
        "validation": f"""
            SELECT `check`, scope, value, target, passed, detail, checked_at
            FROM {c}.ops.validation_results
            WHERE version = :version AND run_id = (
              SELECT run_id FROM {c}.ops.validation_results WHERE version = :version
              ORDER BY checked_at DESC LIMIT 1)""",
    }


def _from_databricks(name: str, cfg: dict) -> pd.DataFrame:
    from databricks import sql   # imported here so demo mode doesn't need the connector

    db = _secrets("databricks")
    missing = [k for k in ("server_hostname", "http_path", "access_token") if not db.get(k)]
    if missing:
        raise DataError(f"Missing Databricks secrets: {', '.join(missing)}. "
                        "Add them in the app's Settings → Secrets.")
    with sql.connect(server_hostname=db["server_hostname"], http_path=db["http_path"],
                     access_token=db["access_token"]) as conn, conn.cursor() as cur:
        cur.execute(queries(cfg["catalog"])[name], {"version": cfg["version"]})
        return cur.fetchall_arrow().to_pandas()


def load(name: str) -> pd.DataFrame:
    """One dataset. Cached per data source and model version, so changing either reloads."""
    cfg = settings()
    return _load(name, cfg["demo_dir"], cfg["catalog"], cfg["version"])


@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Loading ratings (the warehouse may take a minute to wake up)…")
def _load(name: str, demo_dir, catalog: str, version: str) -> pd.DataFrame:
    cfg = {"demo_dir": demo_dir, "catalog": catalog, "version": version}
    try:
        if cfg["demo_dir"]:
            df = pd.read_parquet(os.path.join(cfg["demo_dir"], f"{name}.parquet"))
        else:
            df = _from_databricks(name, cfg)
    except DataError:
        raise
    except Exception as e:
        if name in OPTIONAL:
            return pd.DataFrame()
        raise DataError(f"Couldn't load {name}: {e}") from e
    for col in ("match_date", "last_match_date", "latest_match", "first_match_date", "fitted_to"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def load_all() -> dict:
    return {name: load(name) for name in ("ratings", "matches", "form", "meta", "validation")}
