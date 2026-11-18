"""Match card: every player in a match, rated."""
import pandas as pd
import streamlit as st

import data
import stats
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
ground = m["venue_name"] if pd.notna(m.get("venue_name")) else m["venue"]
city = m["city"] if pd.notna(m["city"]) and m["city"] and str(m["city"]) not in str(ground) else None
where = ", ".join(str(x) for x in (ground, city) if x is not None and pd.notna(x) and x)
stage = m.get("event_stage")
event = f"{m['event_name'] or comp}" + (f", {stage}" if isinstance(stage, str) and stage else "")
st.caption(f"{event} · {m['match_date']:%d %B %Y} · {where}")
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
        "Bowling": [ui.bowling_with_par(*x) for x in zip(side["wickets"], side["runs_conceded"],
                                                         side["balls_bowled"], side["par_runs_conceded"],
                                                         side["par_wickets"])],
        "Fielding": [ui.fielding_line(*x) for x in zip(side["catches"], side["stumpings"], side["run_outs"])],
    })
    with col:
        st.subheader(team)
        st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True)
st.caption("★ official player of the match · \"–\" too little involvement to rate · "
           "par = wickets/runs a typical bowler gets from the same overs with the same wickets down, "
           "adjusted for competition, season and ground")

# --- momentum: where each innings was heading, ball by ball
mom = stats.momentum(data.match_balls(mid))
if not mom.empty:
    import altair as alt

    st.subheader("Momentum")
    names = ratings.drop_duplicates("player_id").set_index("player_id")["name"]
    mom = mom.assign(
        team=mom["batting_team"],
        score=mom["runs_after"].astype(int).astype(str) + "/" + mom["wickets_after"].astype(int).astype(str),
        what=[stats.describe_ball(r, names).replace("**", "") for _, r in mom.iterrows()],
    )
    tooltip = [alt.Tooltip("team:N", title="Batting"), alt.Tooltip("over_ball:N", title="Ball"),
               alt.Tooltip("score:N", title="Score"), alt.Tooltip("projected:Q", title="Heading for"),
               alt.Tooltip("what:N", title="What happened")]
    x = alt.X("overs:Q", title="Overs", scale=alt.Scale(domain=[0, 20]))
    y = alt.Y("projected:Q", title="Projected total", scale=alt.Scale(zero=False))
    colour = alt.Color("team:N", title=None, legend=alt.Legend(orient="bottom", labelLimit=300),
                       scale=alt.Scale(domain=[m["inn1_team"], m["inn2_team"]], range=["#1f4e9a", "#e07b00"]))
    line = alt.Chart(mom).mark_line(interpolate="step-after").encode(x=x, y=y, color=colour, tooltip=tooltip)
    wickets = alt.Chart(mom[mom["dismissals"] > 0]).mark_point(filled=True, size=55, shape="circle",
                                                              color="#c0392b").encode(x=x, y=y, tooltip=tooltip)
    layers = [line, wickets]
    if pd.notna(m["inn1_runs"]) and (mom["innings_no"] == 2).any():
        target = pd.DataFrame({"target": [int(m["inn1_runs"]) + 1]})
        layers.append(alt.Chart(target).mark_rule(strokeDash=[5, 4], color="#888").encode(y="target:Q"))
    st.altair_chart(alt.layer(*layers).properties(height=320), use_container_width=True)
    st.caption("Projected total after each ball: runs so far plus what a typical side adds from "
               "there (balls left, wickets down, conditions). Red dots are wickets"
               + ("; the dashed line is the target" if (mom["innings_no"] == 2).any() else "")
               + (" (a rain-revised target may differ)." if pd.notna(m.get("method")) else "."))

    st.subheader("Biggest swing balls")
    swings = stats.swing_balls(mom)
    st.dataframe(pd.DataFrame({
        "Batting": swings["team"].values,
        "Ball": swings["over_ball"].values,
        "What happened": [stats.describe_ball(r, names).replace("**", "") for _, r in swings.iterrows()],
        "Score": swings["score"].values,
        "Heading for": [f"{int(a)} → {int(b)}" for a, b in zip(swings["projected_before"], swings["projected"])],
        "Swing (runs)": [f"{v:+.1f}" for v in swings["runs_added"]],
    }), hide_index=True, use_container_width=True)
    st.caption("The balls that moved the projected total most: wickets cost the batting side the "
               "runs it was expected to add with that batter, boundaries add runs above expectation.")
