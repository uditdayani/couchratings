"""About: how the ratings work, and how well they hold up."""
import json

import pandas as pd
import streamlit as st

import data
import ui

st.title("About these ratings")
st.markdown("""
Every T20 player gets a **0–10 rating** for each match, like Sofascore's football ratings.

**How it works**
1. Every ball is valued by how much it changed the batting side's **expected runs**, based on balls
   left and wickets down, adjusted for how high-scoring that competition, season and ground are.
2. The striker earns their runs plus the cost of using up the ball. The bowler earns the opposite.
   A wicket's cost is shared between the bowler and fielders (caught 80/20, stumped 70/30, run outs to the fielders).
3. Each player's match total is ranked against every performance **in the same competition**, then
   mapped to a rating: median 6.5, top 10% 8.0+, top 1% 9.3+, the best ever 10.
4. **Form** is a weighted average of the last 10 ratings, recent matches counting most.

Players who faced fewer than 5 balls, bowled less than an over and took no part in a dismissal show "–".
""")

tops = [10.0] + [low for low, _, _ in ui.BANDS[:-1]]
bands = pd.DataFrame([{
    "Rating": (low + top) / 2 if low else 5.0,
    "Band": label,
    "Range": f"{low:.1f}–{top:.1f}" if low else f"3.0–{top:.1f}",
} for (low, label, _), top in zip(ui.BANDS, tops)])
st.dataframe(ui.style_ratings(bands), hide_index=True)

meta = data.load("meta")
if not meta.empty:
    v = meta.iloc[0]
    st.subheader("Data and model")
    c1, c2, c3 = st.columns(3)
    c1.metric("Model version", v["version"])
    c2.metric("Latest match", f"{v['latest_match']:%d %b %Y}" if pd.notna(v["latest_match"]) else "–")
    c3.metric("Expected-runs table fitted on", f"{int(v['fitted_matches']):,} matches")
    try:
        params = json.loads(v["params"] or "{}")
        st.caption(f"Fitted on matches from {params.get('min_date') or 'all seasons'} onwards.")
    except (TypeError, ValueError):
        pass

checks = data.load("validation")
if not checks.empty:
    st.subheader("How well the ratings hold up")
    passed = int(checks["passed"].sum())
    st.caption(f"{passed} of {len(checks)} validation checks pass for this model version.")
    st.dataframe(checks[["check", "scope", "value", "target", "passed", "detail"]].rename(columns=str.capitalize),
                 hide_index=True, use_container_width=True)

st.subheader("Known limitations")
st.markdown("""
- Short, match-turning cameos are underrated: ratings value runs above expectation, not *when* they swing a match.
- Ratings ignore the result, so a great performance in a losing side still rates highly.
- Opposition strength isn't modelled beyond rating each competition separately.
- Fielding only counts catches, stumpings and run outs; drops and runs saved aren't in the data.
""")
st.markdown("Source code and method: see the project's GitHub README.")
