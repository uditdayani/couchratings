"""Player page: form, consistency, teams, venues, matchups and the full match history."""
import pandas as pd
import streamlit as st

import data
import stats
import ui

ratings, form, matches = data.load("ratings"), data.load("form"), data.load("matches")
rated = ratings[ratings["involved"]]

players = (rated.groupby(["player_id", "name"], dropna=False)
           .agg(last=("match_date", "max"), matches=("match_id", "nunique"))
           .reset_index().sort_values("last", ascending=False))
if players.empty:
    st.info("No rated players yet.")
    st.stop()
teams = ui.player_teams(rated)
label = {r.player_id: f"{r.name} ({teams['label'].get(r.player_id, '')})" for r in players.itertuples()}

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

photo = data.load("photos")
photo = photo[photo["player_id"] == pid] if not photo.empty else photo
head = st.container()
if not photo.empty:
    pic, head = st.columns([1, 4], vertical_alignment="center")
    p = photo.iloc[0]
    pic.image(p["image_url"], use_container_width=True)
    credit = " · ".join(str(x) for x in (p["artist"], p["license"]) if isinstance(x, str) and x)
    pic.caption(f"[Photo]({p['page_url']}){': ' + credit if credit else ''}, via Wikimedia Commons")
head.title(players.loc[players["player_id"] == pid, "name"].iloc[0])
country = teams["country"].get(pid) if pid in teams.index else None
club = teams["club"].get(pid) if pid in teams.index else None
parts = [str(country)] if isinstance(country, str) else []
if isinstance(club, str):
    parts.append(f"last IPL team: {club}" if parts else club)
head.caption(" · ".join(parts + [f"{len(mr)} rated T20 matches"]))

# --- headline numbers
my_form = form[form["player_id"] == pid].set_index("scope")
c1, c2, c3, c4 = st.columns(4)
overall = my_form["form_rating"].get("All")
c1.metric("Current form", ui.fmt_rating(overall) if overall is not None else "–",
          help="Weighted average of the last 10 ratings (needs 3+ rated matches).")
c2.metric("Average rating", ui.fmt_rating(mr["rating"].mean()) if len(mr) else "–")
c3.metric("Best rating", ui.fmt_rating(mr["rating"].max()) if len(mr) else "–")
c4.metric("Ratings of 9+", int((mr["rating"] >= 9).sum()))

bm = stats.player_big_matches(ratings, matches, pid)
if bm["matches"]:
    diff = "" if bm["difference"] is None else f", {bm['difference']:+.2f} vs his other matches"
    st.caption(f"Big matches: {bm['average']:.2f} average in {bm['matches']} knockout "
               f"match{'es' if bm['matches'] > 1 else ''}{diff}")

per_comp = my_form.drop(index="All", errors="ignore")
if not per_comp.empty:
    st.caption("Form by competition: " + " · ".join(
        f"{scope} {r.form_rating:.2f} ({int(r.matches_counted)} matches)" for scope, r in per_comp.iterrows()))

# --- consistency
if len(mr) >= 5:
    c = stats.consistency(mr).loc[pid]
    st.subheader("Consistency")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Median rating", f"{c['median']:.1f}", help="Half his matches rate above this, half below.")
    k2.metric("Spread", f"{c['spread']:.2f}",
              help="Standard deviation of his ratings: lower means steadier. Most regulars are 0.9–1.4.")
    k3.metric("Good days", f"{c['pct_good']:.0f}%", help=f"Matches rated {stats.GOOD_DAY:.1f} or higher.")
    k4.metric("Off days", f"{c['pct_off']:.0f}%", help=f"Matches rated {stats.OFF_DAY:.1f} or lower.")

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

# --- teams and venues
left, right = st.columns(2)
with left:
    st.subheader("Teams played for")
    st.dataframe(stats.teams_played_for(ratings, pid), hide_index=True, use_container_width=True)
with right:
    st.subheader("Best grounds")
    pv = stats.player_venues(ratings, matches, pid)
    if pv.empty:
        st.caption("Needs 3+ rated matches at a ground.")
    else:
        names = stats.venue_names(matches)
        st.dataframe(pd.DataFrame({
            "Ground": pv["venue_key"].map(names).values,
            "Matches": pv["matches"].values,
            "Rating": pv["average"].values,
            "vs elsewhere": [f"{d:+.2f}" for d in pv["difference"]],
        }).pipe(ui.style_ratings), hide_index=True, use_container_width=True)
        st.caption("Average rating at each ground (3+ rated matches), and the difference from "
                   "his average everywhere else.")

# --- matchups
matchups = data.load("matchups")
if not matchups.empty:
    names_by_id = ratings.drop_duplicates("player_id").set_index("player_id")["name"]
    st.subheader("Head-to-head")
    t1, t2 = st.tabs(["As a batter", "As a bowler"])
    for tab, role, who in ((t1, "batter", "bowlers"), (t2, "bowler", "batters")):
        opp = stats.opponents(matchups, pid, as_role=role)
        with tab:
            if opp.empty:
                st.caption(f"No {who} with 12+ balls against him yet.")
                continue
            # From this player's side: as a bowler, a batter scoring below par is good for him.
            sign = 1 if role == "batter" else -1
            opp = opp.assign(edge=sign * opp["vs_par_per_over"])
            show = opp.sort_values("balls", ascending=False).head(15)
            st.dataframe(pd.DataFrame({
                who.capitalize()[:-1]: show["opponent_id"].map(names_by_id).values,
                "Balls": show["balls"].values, "Runs": show["runs"].values,
                "Outs": show["dismissals"].values, "Strike rate": show["strike_rate"].values,
                "Runs vs par per over": [f"{e:+.2f}" for e in show["edge"]],
            }), hide_index=True, use_container_width=True)
            st.caption(f"His most frequent {who}. Runs vs par is from his side: positive means he "
                       "came out on top. Open the Head-to-head page for any pairing.")

# --- full match history
st.subheader("Match history")
f1, f2, f3, f4 = st.columns([3, 2, 2, 1])
comps = sorted(mine["competition"].dropna().unique())
pick_comps = f1.multiselect("Competition", comps, key="hist_comp")
years = sorted(mine["match_date"].dt.year.unique(), reverse=True)
pick_years = f2.multiselect("Year", years, key="hist_year")
role = f3.radio("Role", ["All", "Batted", "Bowled"], horizontal=True, key="hist_role")
only_rated = f4.toggle("Rated only", key="hist_rated")
log = stats.filter_history(mine, pick_comps, pick_years, role, only_rated)

PAGE = 25
pages = stats.n_pages(len(log), PAGE)
page = st.number_input(f"Page (of {pages})", min_value=1, max_value=pages, value=1, step=1, key="hist_page") \
    if pages > 1 else 1
shown = stats.page_of(log, page, PAGE).reset_index(drop=True)
table = pd.DataFrame({
    "Date": shown["match_date"].dt.strftime("%d %b %Y"),
    "Competition": shown["competition"],
    "Team": shown["team"],
    "Opponent": shown["opponent"],
    "Rating": shown["rating"],
    "Batting": [ui.batting_line(*x) for x in zip(shown["runs"], shown["balls_faced"], shown["dismissed"])],
    "Bowling": [ui.bowling_line(*x) for x in zip(shown["wickets"], shown["runs_conceded"], shown["balls_bowled"])],
    "Fielding": [ui.fielding_line(*x) for x in zip(shown["catches"], shown["stumpings"], shown["run_outs"])],
    "Impact (runs)": shown["total_impact"].map(lambda v: f"{v:+.1f}"),
})
st.caption(f"{len(log)} matches · select a row to open the match. \"–\" means too little involvement to rate.")
event = st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True,
                     on_select="rerun", selection_mode="single-row", key="player_log")
rows = event.selection.rows if event else []
if rows:
    st.session_state["match_id"] = shown.loc[rows[0], "match_id"]
    st.switch_page("views/match.py")
