"""Head-to-head: one batter against one bowler, ball by ball."""
import pandas as pd
import streamlit as st

import data
import stats
import ui

st.title("Head-to-head")
matchups, ratings = data.load("matchups"), data.load("ratings")
if matchups.empty:
    st.info("Head-to-head data isn't available yet: it's built by the ratings step "
            "(notebook 06) and appears after the next run.")
    st.stop()

names = ratings.drop_duplicates("player_id").set_index("player_id")["name"]
teams = ui.player_teams(ratings[ratings["involved"]])["label"]


def label(pid):
    team = teams.get(pid, "")
    return f"{names.get(pid, pid)} ({team})" if team else names.get(pid, pid)


batters = matchups.groupby("batter_id")["balls"].sum().sort_values(ascending=False)
batter_ids = batters.index.tolist()
want_bat = st.query_params.get("batter")
c1, c2 = st.columns(2)
batter = c1.selectbox("Batter", batter_ids, index=batter_ids.index(want_bat) if want_bat in batter_ids else 0,
                      format_func=label)
faced = (matchups[matchups["batter_id"] == batter].groupby("bowler_id")["balls"].sum()
         .sort_values(ascending=False))
bowler_ids = faced.index.tolist()
want_bowl = st.query_params.get("bowler")
bowler = c2.selectbox("Bowler", bowler_ids,
                      index=bowler_ids.index(want_bowl) if want_bowl in bowler_ids else 0,
                      format_func=lambda b: f"{label(b)} · {int(faced[b])} balls")
st.query_params["batter"], st.query_params["bowler"] = batter, bowler

m = stats.matchup(matchups, batter, bowler)
total = m[m["competition"] == "All"].iloc[0]
st.subheader(f"{names.get(batter, batter)} v {names.get(bowler, bowler)}")

k = st.columns(5)
k[0].metric("Runs (balls)", f"{int(total['runs'])} ({int(total['balls'])})")
k[1].metric("Dismissals", int(total["dismissals"]))
k[2].metric("Strike rate", "–" if pd.isna(total["strike_rate"]) else f"{total['strike_rate']:.0f}")
k[3].metric("Dot balls", "–" if pd.isna(total["dot_pct"]) else f"{total['dot_pct']:.0f}%")
edge = total["vs_par_per_over"]
k[4].metric("Runs vs par per over", "–" if pd.isna(edge) else f"{edge:+.2f}",
            help="Runs the batter scored against this bowler, minus what a typical batter scores "
                 "from the same overs with the same wickets down, per 6 balls. Getting out counts "
                 "against him. Positive: better than a typical batter would have done.")

ctx = stats.context(matchups, batter, bowler)
bat_name, bowl_name = names.get(batter, batter), names.get(bowler, bowler)


def summary(rec):
    if rec["balls"] == 0:
        return "no balls yet"
    rpo = 6 * rec["runs"] / rec["balls"]
    outs = (f"out every {rec['balls_per_dismissal']:.0f} balls" if rec["dismissals"]
            else "never out")
    return f"{rpo:.1f} an over, {outs}, {rec['vs_par_per_over']:+.2f} vs par per over"


st.markdown(f"- **{bat_name} against {bowl_name}:** {summary(ctx['pair'])}  \n"
            f"- **Other batters against {bowl_name}:** {summary(ctx['others_v_bowler'])}  \n"
            f"- **{bat_name} against other bowlers:** {summary(ctx['batter_v_others'])}")
v = stats.verdict(ctx)
st.markdown({
    "too few": "Too few balls between them to say who has the upper hand.",
    "batter": f"**{bat_name}** has had the upper hand: he has done better against {bowl_name} "
              "than other batters have.",
    "bowler": f"**{bowl_name}** has had the upper hand: {bat_name} has done worse against him "
              "than other batters have.",
    "even": f"Honours roughly even: {bat_name} has done about as well against {bowl_name} as "
            "other batters have.",
}[v])

table = pd.DataFrame({
    "Competition": m["competition"], "Matches": m["matches"].astype(int),
    "Balls": m["balls"].astype(int), "Runs": m["runs"].astype(int),
    "Outs": m["dismissals"].astype(int), "4s": m["fours"].astype(int), "6s": m["sixes"].astype(int),
    "Strike rate": m["strike_rate"], "Average": m["average"], "Dot %": m["dot_pct"],
    "Runs vs par per over": m["vs_par_per_over"],
})
st.dataframe(table, hide_index=True, use_container_width=True)
st.caption("Balls exclude wides; outs count only dismissals credited to the bowler (not run outs). "
           "\"Matches\" in the All row is the total across competitions.")
