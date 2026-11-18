"""Team of the season: the best XI of one year of one competition."""
import pandas as pd
import streamlit as st

import data
import stats
import ui

st.title("Team of the season")
ratings = data.load("ratings")
rated = ratings[ratings["involved"]]
if rated.empty:
    st.info("No rated matches yet.")
    st.stop()

c1, c2, c3 = st.columns([3, 2, 2])
comps = sorted(rated["competition"].dropna().unique())
comp = c1.selectbox("Competition", comps,
                    index=comps.index("Indian Premier League") if "Indian Premier League" in comps else 0)
years = sorted(rated.loc[rated["competition"] == comp, "match_date"].dt.year.unique(), reverse=True)
year = c2.selectbox("Year", years)
min_m = c3.number_input("Minimum rated matches", 2, 20, 6 if comp == "Indian Premier League" else 4)

xi = stats.team_of_season(ratings, comp, int(year), int(min_m))
if xi.empty or len(xi) < 11:
    st.info("Not enough players with that many rated matches this year. Try a lower minimum.")
    st.stop()

st.caption("1 wicketkeeper, 4 batters, 2 all-rounders and 4 bowlers, each the best in their role. "
           "Players are ranked by their average rating, pulled towards 6.5 by three average "
           "matches so a long, very good season beats a short burst. Roles come from the season: "
           "bowlers average 2+ overs a match, all-rounders also face 10+ balls, and the keeper "
           "made a stumping or conceded byes. Select a row to open the player.")
table = pd.DataFrame({
    "#": range(1, 12), "Player": xi["name"], "Role": xi["role"].str.capitalize(),
    "Team": xi["team"], "Rating": xi["average"], "Matches": xi["matches"],
    "Best": xi["best"].map(ui.fmt_rating),
})
ev = st.dataframe(ui.style_ratings(table), hide_index=True, use_container_width=True,
                  on_select="rerun", selection_mode="single-row", key="tots")
rows = ev.selection.rows if ev else []
if rows:
    st.session_state["player_id"] = xi.loc[rows[0], "player_id"]
    st.switch_page("views/player.py")

pool = stats.season_players(ratings, comp, int(year), int(min_m))
bench = pool[~pool["player_id"].isin(xi["player_id"])]
if not bench.empty:
    st.subheader("Next in line")
    nxt = bench.groupby("role").head(2).sort_values(["role", "score"], ascending=[True, False])
    st.dataframe(ui.style_ratings(pd.DataFrame({
        "Player": nxt["name"], "Role": nxt["role"].str.capitalize(), "Team": nxt["team"],
        "Rating": nxt["average"].round(2), "Matches": nxt["matches"],
    })), hide_index=True, use_container_width=True)
