"""Venue page: how a ground plays, and who plays best there."""
import pandas as pd
import streamlit as st

import data
import stats
import ui

st.title("Venues")
ratings, matches = data.load("ratings"), data.load("matches")
summary = stats.venue_summary(matches)
if summary.empty:
    st.info("No rated matches yet.")
    st.stop()

keys = summary.index.tolist()
wanted = st.session_state.pop("venue", None) or st.query_params.get("venue")
venue = st.selectbox("Ground", keys, index=keys.index(wanted) if wanted in keys else 0,
                     format_func=lambda k: f"{summary.loc[k, 'name']} · {int(summary.loc[k, 'matches'])} matches")
st.query_params["venue"] = venue
v = summary.loc[venue]

st.subheader(v["name"])
here = matches[stats.venue_key(matches) == venue]
cities = ", ".join(sorted(here["city"].dropna().unique()))
st.caption(f"{cities + ' · ' if cities else ''}{int(v['matches'])} rated matches, "
           f"{v['first_match']:%Y}–{v['last_match']:%Y}")

k = st.columns(4)
k[0].metric("Average first innings", "–" if pd.isna(v["avg_first_innings"]) else int(v["avg_first_innings"]),
            help="Full 20-over first innings only, so rain-shortened matches don't drag it down.")
k[1].metric("Average second innings", "–" if pd.isna(v["avg_second_innings"]) else int(v["avg_second_innings"]))
k[2].metric("Won batting first", "–" if pd.isna(v["pct_won_batting_first"]) else f"{v['pct_won_batting_first']:.0f}%")
k[3].metric("Highest total", "–" if pd.isna(v["highest_total"]) else int(v["highest_total"]))

st.subheader("Who plays best here")
min_m = st.slider("Minimum rated matches at this ground", 2, 10, 3)
best = stats.venue_players(ratings, matches, venue, min_matches=min_m).head(25).reset_index(drop=True)
if best.empty:
    st.caption("No player has that many rated matches here yet.")
else:
    st.caption("Average rating here, and the difference from the same player's average everywhere "
               "else. Select a row to open the player.")
    ev = st.dataframe(ui.style_ratings(pd.DataFrame({
        "Player": best["name"], "Rating": best["average"], "Matches": best["matches"],
        "Best here": best["best"].map(ui.fmt_rating), "Elsewhere": best["elsewhere"].map(ui.fmt_rating),
        "Difference": [f"{d:+.2f}" if pd.notna(d) else "–" for d in best["difference"]],
    })), hide_index=True, use_container_width=True, on_select="rerun",
        selection_mode="single-row", key="venue_best")
    rows = ev.selection.rows if ev else []
    if rows:
        st.session_state["player_id"] = best.loc[rows[0], "player_id"]
        st.switch_page("views/player.py")

st.subheader("Best performances here")
top = stats.best_performances(ratings[ratings["match_id"].isin(here["match_id"])], n=10).reset_index(drop=True)
st.dataframe(ui.style_ratings(pd.DataFrame({
    "Player": top["name"], "Rating": top["rating"], "Team": top["team"],
    "Date": top["match_date"].dt.strftime("%d %b %Y"),
    "Batting": [ui.batting_line(*x) for x in zip(top["runs"], top["balls_faced"], top["dismissed"])],
    "Bowling": [ui.bowling_line(*x) for x in zip(top["wickets"], top["runs_conceded"], top["balls_bowled"])],
})), hide_index=True, use_container_width=True)

st.subheader("Recent matches here")
recent = here.sort_values("match_date", ascending=False).head(10).reset_index(drop=True)
ev = st.dataframe(pd.DataFrame({
    "Date": recent["match_date"].dt.strftime("%d %b %Y"),
    "Match": recent["team_1"] + " v " + recent["team_2"],
    "Scores": [f"{ui.score_line(a, b, c)} · {ui.score_line(d, e, f)}" for a, b, c, d, e, f in
               zip(recent["inn1_runs"], recent["inn1_wickets"], recent["inn1_balls"],
                   recent["inn2_runs"], recent["inn2_wickets"], recent["inn2_balls"])],
    "Result": [ui.result_line(r) for _, r in recent.iterrows()],
}), hide_index=True, use_container_width=True, on_select="rerun",
    selection_mode="single-row", key="venue_recent")
rows = ev.selection.rows if ev else []
if rows:
    st.session_state["match_id"] = recent.loc[rows[0], "match_id"]
    st.switch_page("views/match.py")
