"""Week 1 check: can Streamlit query the Databricks SQL warehouse from outside?

Needs a [databricks] section in Streamlit secrets – see .streamlit/secrets.toml.example.
"""
import pandas as pd
import streamlit as st
from databricks import sql

st.set_page_config(page_title="Cricket Ratings – connection check", page_icon="🏏")
st.title("Cricket Ratings – week 1 connection check")

cfg = st.secrets.get("databricks")
if not cfg:
    st.error("No [databricks] secrets found. Copy .streamlit/secrets.toml.example into the app's Secrets.")
    st.stop()


@st.cache_data(ttl=60 * 60 * 24, show_spinner="Waking the SQL warehouse (can take a minute)…")
def run_query(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=cfg["server_hostname"],
        http_path=cfg["http_path"],
        access_token=cfg["access_token"],
    ) as conn, conn.cursor() as cur:
        cur.execute(query)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


try:
    ping = run_query("SELECT current_timestamp() AS warehouse_time")
    st.success(f"Connected. Warehouse time: {ping.iloc[0, 0]}")
except Exception as e:  # show the reason so it can be fixed
    st.error(f"Could not connect: {e}")
    st.stop()

try:
    checks = run_query(f"SELECT * FROM {cfg.get('check_table', 'workspace.ops.connectivity_check')}")
    st.subheader("Results from the week 1 notebook")
    st.dataframe(checks, use_container_width=True, hide_index=True)
    if checks["passed"].all():
        st.balloons()
        st.info("Week 1 checks all passed.")
except Exception as e:
    st.warning(f"Connected, but the check table isn't readable yet – run the notebook first. ({e})")

st.caption("Ball-by-ball data from Cricsheet (cricsheet.org), used under the Open Data Commons Attribution License.")
