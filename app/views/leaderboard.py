"""Form leaderboard: who is playing best right now?"""
import streamlit as st

import data
import ui

st.title("Form leaderboard")
st.caption("Form is a weighted average of a player's last 10 match ratings, with recent matches "
           "counting most. Players need 5+ rated matches and a game in the last 12 months.")

form = data.load("form")
scopes = ["All"] + sorted(s for s in form["scope"].unique() if s != "All")
labels = {"All": "All competitions"}
scope = st.selectbox("Competition", scopes, format_func=lambda s: labels.get(s, s))

board = (form[(form["scope"] == scope) & form["leaderboard_rank"].notna()]
         .sort_values("leaderboard_rank").head(100).reset_index(drop=True))
if board.empty:
    st.info("No ranked players for this competition yet.")
    st.stop()

table = board.assign(
    Rank=board["leaderboard_rank"].astype(int),
    Player=board["name"],
    Rating=board["form_rating"],
    Matches=board["matches_counted"].astype(int),
    Last_played=board["last_match_date"].dt.strftime("%d %b %Y"),
)[["Rank", "Player", "Rating", "Matches", "Last_played"]].rename(columns={"Last_played": "Last played"})

st.caption("Select a row to open the player.")
event = st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True,
                     on_select="rerun", selection_mode="single-row", key="leaderboard")
rows = event.selection.rows if event else []
if rows:
    st.session_state["player_id"] = board.loc[rows[0], "player_id"]
    st.switch_page("views/player.py")
