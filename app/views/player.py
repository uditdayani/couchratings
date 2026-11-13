"""Player page: form, recent ratings, and what drives them."""
import pandas as pd
import streamlit as st

import data
import ui

ratings, form, matches = data.load("ratings"), data.load("form"), data.load("matches")
rated = ratings[ratings["involved"]]

players = (rated.groupby(["player_id", "name"], dropna=False)
           .agg(last=("match_date", "max"), matches=("match_id", "nunique"))
           .reset_index().sort_values("last", ascending=False))
if players.empty:
    st.info("No rated players yet.")
    st.stop()
latest_team = (rated.sort_values("match_date").groupby("player_id")["team"].last())
label = {r.player_id: f"{r.name} ({latest_team.get(r.player_id, '')})" for r in players.itertuples()}

wanted = st.session_state.pop("player_id", None) or st.query_params.get("player")
ids = players["player_id"].tolist()
pid = st.selectbox("Find a player", ids, index=ids.index(wanted) if wanted in ids else 0,
                   format_func=lambda p: label.get(p, p))
st.query_params["player"] = pid

mine = ratings[ratings["player_id"] == pid].merge(
    matches[["match_id", "team_1", "team_2", "event_name"]], on="match_id", how="left")
mine["opponent"] = mine.apply(lambda r: r["team_2"] if r["team"] == r["team_1"] else r["team_1"], axis=1)
mine = mine.sort_values("match_date", ascending=False)
mr = mine[mine["involved"]]

st.title(players.loc[players["player_id"] == pid, "name"].iloc[0])
st.caption(f"{latest_team.get(pid, '')} · {len(mr)} rated T20 matches")

# --- headline numbers
my_form = form[form["player_id"] == pid].set_index("scope")
c1, c2, c3, c4 = st.columns(4)
overall = my_form["form_rating"].get("All")
c1.metric("Current form", ui.fmt_rating(overall) if overall is not None else "–",
          help="Weighted average of the last 10 ratings (needs 3+ rated matches).")
c2.metric("Average rating", ui.fmt_rating(mr["rating"].mean()) if len(mr) else "–")
c3.metric("Best rating", ui.fmt_rating(mr["rating"].max()) if len(mr) else "–")
c4.metric("Ratings of 9+", int((mr["rating"] >= 9).sum()))

per_comp = my_form.drop(index="All", errors="ignore")
if not per_comp.empty:
    st.caption("Form by competition: " + " · ".join(
        f"{scope} {r.form_rating:.2f} ({int(r.matches_counted)} matches)" for scope, r in per_comp.iterrows()))

# --- ratings over time
recent = mr.head(20).sort_values("match_date")
if not recent.empty:
    st.subheader("Last 20 ratings")
    recent = recent.assign(label=recent["team"] + " v " + recent["opponent"] + " · " + recent["competition"])
    st.altair_chart(ui.rating_chart(recent), use_container_width=True)

# --- what drives the ratings
if len(mr):
    st.subheader("Where the impact comes from")
    st.caption("Average runs added per match against what's expected in that situation.")
    i1, i2, i3 = st.columns(3)
    i1.metric("Batting", f"{mr['batting'].mean():+.1f} runs")
    i2.metric("Bowling", f"{mr['bowling'].mean():+.1f} runs")
    i3.metric("Fielding", f"{mr['fielding'].mean():+.1f} runs")

# --- match log
st.subheader("Recent matches")
log = mine.head(30).reset_index(drop=True)
table = pd.DataFrame({
    "Date": log["match_date"].dt.strftime("%d %b %Y"),
    "Competition": log["competition"],
    "Opponent": log["opponent"],
    "Rating": log["rating"],
    "Batting": [ui.batting_line(*x) for x in zip(log["runs"], log["balls_faced"], log["dismissed"])],
    "Bowling": [ui.bowling_line(*x) for x in zip(log["wickets"], log["runs_conceded"], log["balls_bowled"])],
    "Fielding": [ui.fielding_line(*x) for x in zip(log["catches"], log["stumpings"], log["run_outs"])],
    "Impact (runs)": log["total_impact"].round(1),
})
st.caption("Select a row to open the match. \"–\" means too little involvement to rate.")
event = st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True,
                     on_select="rerun", selection_mode="single-row", key="player_log")
rows = event.selection.rows if event else []
if rows:
    st.session_state["match_id"] = log.loc[rows[0], "match_id"]
    st.switch_page("views/match.py")
