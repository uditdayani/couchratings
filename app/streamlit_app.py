"""Slogonomics – Streamlit dashboard (entry point).

Data comes from the Parquet export ([data] url) or, without it, the Databricks SQL
warehouse ([databricks] secrets); optionally [app] model_version (default "v3").
See .streamlit/secrets.toml.example and app/data.py.
"""
import os

import streamlit as st

import data
import ui

st.set_page_config(page_title="Slogonomics", page_icon="🏏", layout="wide")
st.logo(os.path.join(os.path.dirname(__file__), "static", "logo.svg"), size="large")

pages = [
    st.Page("views/leaderboard.py", title="Form leaderboard", icon=":material/leaderboard:", default=True),
    st.Page("views/player.py", title="Player", icon=":material/person:"),
    st.Page("views/match.py", title="Match", icon=":material/sports_cricket:"),
    st.Page("views/records.py", title="Records", icon=":material/emoji_events:"),
    st.Page("views/team_of_season.py", title="Team of the season", icon=":material/groups:"),
    st.Page("views/head_to_head.py", title="Head-to-head", icon=":material/swords:"),
    st.Page("views/venues.py", title="Venues", icon=":material/stadium:"),
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
