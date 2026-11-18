"""All data the dashboard shows, loaded once and cached for 12 hours.

Three sources, first match wins:
  1. Demo: the environment variable CRICKET_RATINGS_DATA_DIR names a folder of
     <dataset>.parquet files (used by the tests, and to run the app without anything else).
  2. Parquet export: secrets `[data] url` (or CRICKET_RATINGS_DATA_URL) is a public folder of
     <dataset>.parquet files, published daily by notebook 09_export_parquet as a GitHub
     release. No warehouse, no token.
  3. Databricks: the SQL warehouse, using the queries in queries.py and the `[databricks]`
     secrets.

`load_all` loads what every page needs up front; larger, page-specific datasets
(`matchups`, `balls`) load the first time a page asks for them.
"""
import io
import os

import pandas as pd
import requests

from queries import CORE, balls_query, matches_query, queries  # noqa: F401
import streamlit as st

CACHE_SECONDS = 12 * 3600
OPTIONAL = {"validation", "matchups", "balls", "photos"}   # the app works without these


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
            "version": app.get("model_version", "v3"),
            "demo_dir": os.environ.get("CRICKET_RATINGS_DATA_DIR"),
            "data_url": os.environ.get("CRICKET_RATINGS_DATA_URL") or _secrets("data").get("url")}


def _from_databricks(name: str, cfg: dict) -> pd.DataFrame:
    from databricks import sql   # imported here so demo mode doesn't need the connector

    db = _secrets("databricks")
    missing = [k for k in ("server_hostname", "http_path", "access_token") if not db.get(k)]
    if missing:
        raise DataError(f"Missing Databricks secrets: {', '.join(missing)}. "
                        "Add them in the app's Settings → Secrets.")
    sql_text = queries(cfg["catalog"])[name]
    with sql.connect(server_hostname=db["server_hostname"], http_path=db["http_path"],
                     access_token=db["access_token"]) as conn, conn.cursor() as cur:
        try:
            cur.execute(sql_text, {"version": cfg["version"]})
        except Exception:
            if name != "matches":
                raise
            # Before the venue register is loaded (03_silver_build), show venues as written.
            cur.execute(matches_query(cfg["catalog"], venue_register=False), {"version": cfg["version"]})
        return cur.fetchall_arrow().to_pandas()


def _from_url(name: str, base: str) -> pd.DataFrame:
    r = requests.get(f"{base.rstrip('/')}/{name}.parquet", timeout=120)
    r.raise_for_status()
    return pd.read_parquet(io.BytesIO(r.content))


def load(name: str) -> pd.DataFrame:
    """One dataset. Cached per data source and model version, so changing either reloads."""
    cfg = settings()
    return _load(name, cfg["demo_dir"], cfg["data_url"], cfg["catalog"], cfg["version"])


@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Loading ratings…")
def _load(name: str, demo_dir, data_url, catalog: str, version: str) -> pd.DataFrame:
    cfg = {"demo_dir": demo_dir, "catalog": catalog, "version": version}
    try:
        if demo_dir:
            df = pd.read_parquet(os.path.join(demo_dir, f"{name}.parquet"))
        elif data_url:
            df = _from_url(name, data_url)
        elif name == "balls":
            return pd.DataFrame()            # from the warehouse, one match at a time instead
        else:
            df = _from_databricks(name, cfg)
    except DataError:
        raise
    except Exception as e:
        if name in OPTIONAL:
            return pd.DataFrame()
        raise DataError(f"Couldn't load {name}: {e}") from e
    if name == "matches":                    # demo data or tables without the venue register
        for col in ("venue_id", "venue_name", "event_stage"):
            if col not in df.columns:
                df[col] = None
    if name == "ratings":                    # tables built before par existed
        for col in ("par_runs_conceded", "par_wickets"):
            if col not in df.columns:
                df[col] = None
    for col in ("match_date", "last_match_date", "latest_match", "first_match_date", "fitted_to"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def match_balls(match_id: str) -> pd.DataFrame:
    """Ball-by-ball values for one match (momentum chart). Empty if not available."""
    cfg = settings()
    if cfg["demo_dir"] or cfg["data_url"]:
        balls = load("balls")
        return balls[balls["match_id"] == match_id] if not balls.empty else balls
    return _match_balls_from_databricks(match_id, cfg["catalog"], cfg["version"])


@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Loading the ball-by-ball…", max_entries=200)
def _match_balls_from_databricks(match_id: str, catalog: str, version: str) -> pd.DataFrame:
    from databricks import sql
    db = _secrets("databricks")
    try:
        with sql.connect(server_hostname=db["server_hostname"], http_path=db["http_path"],
                         access_token=db["access_token"]) as conn, conn.cursor() as cur:
            cur.execute(balls_query(catalog), {"version": version, "match_id": match_id})
            return cur.fetchall_arrow().to_pandas()
    except Exception:
        return pd.DataFrame()


def load_all() -> dict:
    return {name: load(name) for name in CORE}
