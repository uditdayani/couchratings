"""Cricket Player Ratings – Streamlit dashboard (entry point).

Needs [databricks] secrets (server_hostname, http_path, access_token; optional catalog)
and optionally [app] model_version (default "v2"). See .streamlit/secrets.toml.example.
"""
import streamlit as st

import data
import ui

st.set_page_config(page_title="Cricket Player Ratings", page_icon="🏏", layout="wide")

pages = [
    st.Page("views/leaderboard.py", title="Form leaderboard", icon=":material/leaderboard:", default=True),
    st.Page("views/player.py", title="Player", icon=":material/person:"),
    st.Page("views/match.py", title="Match", icon=":material/sports_cricket:"),
    st.Page("views/about.py", title="About", icon=":material/info:"),
]
page = st.navigation(pages)

try:
    data.load_all()
except data.DataError as e:
    st.error(f"The ratings couldn't be loaded. {e}")
    st.stop()

page.run()
st.divider()
st.caption(ui.ATTRIBUTION)
