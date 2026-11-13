"""Match card: every player in a match, rated."""
import pandas as pd
import streamlit as st

import data
import ui

ratings, matches = data.load("ratings"), data.load("matches")
comp_of = ratings.drop_duplicates("match_id").set_index("match_id")["competition"]
matches = matches.assign(competition=matches["match_id"].map(comp_of)).sort_values("match_date", ascending=False)
if matches.empty:
    st.info("No rated matches yet.")
    st.stop()

wanted = st.session_state.pop("match_id", None) or st.query_params.get("match")
start = matches[matches["match_id"] == wanted].iloc[0] if wanted in set(matches["match_id"]) else matches.iloc[0]

comps = sorted(matches["competition"].dropna().unique())
c1, c2, c3 = st.columns([2, 1, 3])
comp = c1.selectbox("Competition", comps, index=comps.index(start["competition"]))
in_comp = matches[matches["competition"] == comp]
years = sorted(in_comp["match_date"].dt.year.unique(), reverse=True)
year = c2.selectbox("Year", years, index=years.index(start["match_date"].year) if start["competition"] == comp else 0)
options = in_comp[in_comp["match_date"].dt.year == year]
ids = options["match_id"].tolist()
names = {r.match_id: f"{r.match_date:%d %b} · {r.team_1} v {r.team_2}" for r in options.itertuples()}
mid = c3.selectbox("Match", ids, index=ids.index(start["match_id"]) if start["match_id"] in ids else 0,
                   format_func=lambda m: names[m])
st.query_params["match"] = mid
m = matches[matches["match_id"] == mid].iloc[0]

# --- header
st.title(f"{m['team_1']} v {m['team_2']}")
where = ", ".join(str(x) for x in (m["venue"], m["city"]) if pd.notna(x) and x)
st.caption(f"{m['event_name'] or comp} · {m['match_date']:%d %B %Y} · {where}")
s1, s2 = st.columns(2)
s1.metric(str(m["inn1_team"] or ""), ui.score_line(m["inn1_runs"], m["inn1_wickets"], m["inn1_balls"]) or "–")
s2.metric(str(m["inn2_team"] or ""), ui.score_line(m["inn2_runs"], m["inn2_wickets"], m["inn2_balls"]) or "–")
st.markdown(f"**{ui.result_line(m)}**")

players = ratings[ratings["match_id"] == mid].copy()
pom_ids = set(m["player_of_match_ids"]) if m["player_of_match_ids"] is not None else set()
top = players[players["involved"]].sort_values(["rating", "total_impact"], ascending=False).head(1)
notes = []
if not top.empty:
    notes.append(f"Top rated: **{top.iloc[0]['name']}** ({ui.fmt_rating(top.iloc[0]['rating'])})")
pom = players[players["player_id"].isin(pom_ids)]["name"].tolist()
if pom:
    notes.append("Official player of the match: **" + ", ".join(pom) + "**")
if notes:
    st.markdown(" · ".join(notes))

# --- one card per team
cols = st.columns(2)
for col, team in zip(cols, [m["team_1"], m["team_2"]]):
    side = players[players["team"] == team].sort_values(["involved", "rating", "total_impact"],
                                                         ascending=[False, False, False])
    table = pd.DataFrame({
        "Player": [n + (" ★" if p in pom_ids else "") for n, p in zip(side["name"], side["player_id"])],
        "Rating": side["rating"].values,
        "Batting": [ui.batting_line(*x) for x in zip(side["runs"], side["balls_faced"], side["dismissed"])],
        "Bowling": [ui.bowling_line(*x) for x in zip(side["wickets"], side["runs_conceded"], side["balls_bowled"])],
        "Fielding": [ui.fielding_line(*x) for x in zip(side["catches"], side["stumpings"], side["run_outs"])],
    })
    with col:
        st.subheader(team)
        st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True)
st.caption("★ official player of the match · \"–\" too little involvement to rate")
