"""Records: the best performances, players and seasons."""
import pandas as pd
import streamlit as st

import data
import stats
import ui

st.title("Records")
ratings, matches = data.load("ratings"), data.load("matches")
opponent = matches.set_index("match_id")[["team_1", "team_2"]]

comps = ["All"] + sorted(ratings["competition"].dropna().unique())
comp = st.selectbox("Competition", comps, format_func=lambda c: "All competitions" if c == "All" else c)
scope = None if comp == "All" else comp


def open_on_select(event, frame: pd.DataFrame, column: str, page: str, state_key: str):
    rows = event.selection.rows if event else []
    if rows:
        st.session_state[state_key] = frame.iloc[rows[0]][column]
        st.switch_page(page)


tabs = st.tabs(["Best performances", "Most 9+ ratings", "Best career average",
                "Best seasons", "Most consistent", "Big-match players"])

with tabs[0]:
    best = stats.best_performances(ratings, scope).reset_index(drop=True)
    vs = [opponent.loc[m, "team_2"] if t == opponent.loc[m, "team_1"] else opponent.loc[m, "team_1"]
          for m, t in zip(best["match_id"], best["team"])] if not best.empty else []
    table = pd.DataFrame({
        "Player": best["name"], "Rating": best["rating"],
        "For": best["team"], "Against": vs,
        "Date": best["match_date"].dt.strftime("%d %b %Y"),
        "Batting": [ui.batting_line(*x) for x in zip(best["runs"], best["balls_faced"], best["dismissed"])],
        "Bowling": [ui.bowling_line(*x) for x in zip(best["wickets"], best["runs_conceded"], best["balls_bowled"])],
        "Impact (runs)": best["total_impact"].map(lambda v: f"{v:+.1f}"),
    })
    st.caption("The 50 highest match ratings (ties broken by impact in runs). Select a row to open the match.")
    ev = st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True,
                      on_select="rerun", selection_mode="single-row", key="rec_best")
    open_on_select(ev, best, "match_id", "views/match.py", "match_id")

with tabs[1]:
    most = stats.most_nine_plus(ratings, scope).reset_index(drop=True)
    st.caption("Players with the most match ratings of 9 or higher. Select a row to open the player.")
    ev = st.dataframe(pd.DataFrame({"Player": most["name"], "9+ ratings": most["nine_plus"],
                                    "Rated matches": most["matches"], "% of matches": most["pct"]}),
                      hide_index=True, use_container_width=True, on_select="rerun",
                      selection_mode="single-row", key="rec_nine")
    open_on_select(ev, most, "player_id", "views/player.py", "player_id")

with tabs[2]:
    avg = stats.best_averages(ratings, scope).reset_index(drop=True)
    st.caption("Highest average rating, minimum 30 rated matches. Select a row to open the player.")
    ev = st.dataframe(ui.style_ratings(pd.DataFrame({"Player": avg["name"], "Rating": avg["average"],
                                                     "Rated matches": avg["matches"]})),
                      hide_index=True, use_container_width=True, on_select="rerun",
                      selection_mode="single-row", key="rec_avg")
    open_on_select(ev, avg, "player_id", "views/player.py", "player_id")

with tabs[3]:
    seasons = stats.best_seasons(ratings, scope).reset_index(drop=True)
    st.caption("Highest average rating in one year of one competition, minimum 8 rated matches.")
    ev = st.dataframe(ui.style_ratings(pd.DataFrame({
        "Player": seasons["name"], "Rating": seasons["average"], "Year": seasons["year"].astype(str),
        "Competition": seasons["competition"], "Rated matches": seasons["matches"]})),
        hide_index=True, use_container_width=True, on_select="rerun",
        selection_mode="single-row", key="rec_season")
    open_on_select(ev, seasons, "player_id", "views/player.py", "player_id")

with tabs[4]:
    cons = stats.most_consistent(ratings if scope is None else ratings[ratings["competition"] == scope])
    cons = cons.reset_index(drop=True)
    st.caption("Smallest spread of ratings among players who average 6.5 or better, minimum 30 rated "
               "matches: the most reliable performers.")
    ev = st.dataframe(ui.style_ratings(pd.DataFrame({
        "Player": cons["name"], "Rating": cons["average"], "Spread": cons["spread"],
        "Good days %": cons["pct_good"], "Off days %": cons["pct_off"],
        "Rated matches": cons["matches"]})),
        hide_index=True, use_container_width=True, on_select="rerun",
        selection_mode="single-row", key="rec_cons")
    open_on_select(ev, cons, "player_id", "views/player.py", "player_id")

with tabs[5]:
    big = stats.big_match_players(ratings, matches, scope).reset_index(drop=True)
    st.caption("Average rating in knockout matches (finals, semi-finals, qualifiers, eliminators), "
               "minimum 3, and the difference from the same player's other matches.")
    if big.empty:
        st.caption("No player has 3+ rated knockout matches yet (needs the match stage: run "
                   "03_silver_build once with rebuild_matches = yes).")
    else:
        ev = st.dataframe(ui.style_ratings(pd.DataFrame({
            "Player": big["name"], "Rating": big["average"], "Knockouts": big["matches"],
            "Best": big["best"].map(ui.fmt_rating), "Other matches": big["other"].map(ui.fmt_rating),
            "Difference": [f"{d:+.2f}" if pd.notna(d) else "–" for d in big["difference"]],
        })), hide_index=True, use_container_width=True, on_select="rerun",
            selection_mode="single-row", key="rec_big")
        open_on_select(ev, big, "player_id", "views/player.py", "player_id")
