"""Formatting and small display helpers shared by the dashboard pages."""
import math
from typing import Optional

import altair as alt
import pandas as pd

# Rating bands, best first: (lowest rating in band, label, colour).
BANDS = [
    (9.0, "Exceptional", "#2563eb"),
    (8.0, "Excellent", "#15803d"),
    (7.0, "Very good", "#65a30d"),
    (6.5, "Good", "#ca8a04"),
    (6.0, "Average", "#ea580c"),
    (0.0, "Poor", "#dc2626"),
]
ATTRIBUTION = ("Ball-by-ball data from [Cricsheet](https://cricsheet.org), used under the "
               "Open Data Commons Attribution License.")


def _missing(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x)) or x is pd.NA or x is pd.NaT


def band(rating) -> tuple:
    """(label, colour) for a rating; ("Not rated", grey) when there is none."""
    if _missing(rating):
        return "Not rated", "#9ca3af"
    for low, label, colour in BANDS:
        if rating >= low:
            return label, colour
    return BANDS[-1][1], BANDS[-1][2]


def fmt_rating(rating) -> str:
    return "–" if _missing(rating) else f"{rating:.1f}"


def overs(balls) -> str:
    """Legal balls in cricket notation: 22 -> "3.4"."""
    balls = int(balls or 0)
    return f"{balls // 6}.{balls % 6}"


def batting_line(runs, balls_faced, dismissed) -> str:
    """"45 (30)", with * when not out; blank if the player didn't bat."""
    if not balls_faced and not dismissed:
        return ""
    return f"{int(runs)}{'' if dismissed else '*'} ({int(balls_faced)})"


def bowling_line(wickets, runs_conceded, balls_bowled) -> str:
    """"2/24 (4.0)"; blank if the player didn't bowl."""
    if not balls_bowled and not runs_conceded:
        return ""
    return f"{int(wickets)}/{int(runs_conceded)} ({overs(balls_bowled)})"


def fielding_line(catches, stumpings, run_outs) -> str:
    parts = [(catches, "ct"), (stumpings, "st"), (run_outs, "ro")]
    return ", ".join(f"{int(n)} {label}" for n, label in parts if n)


def score_line(runs, wickets, balls) -> str:
    if _missing(runs):
        return ""
    wkts = int(wickets) if not _missing(wickets) else 0
    return f"{int(runs)}{'' if wkts >= 10 else f'/{wkts}'} ({overs(balls)} ov)"


def result_line(m) -> str:
    """Human-readable result for a row of the matches dataset."""
    rain = f" ({m['method']})" if not _missing(m.get("method")) else ""
    if m.get("result") == "win":
        if not _missing(m.get("win_by_runs")):
            n, unit = int(m["win_by_runs"]), "run"
        elif not _missing(m.get("win_by_wickets")):
            n, unit = int(m["win_by_wickets"]), "wicket"
        else:
            return f"{m['winner']} won{rain}"
        return f"{m['winner']} won by {n} {unit}{'s' if n != 1 else ''}{rain}"
    if m.get("result") == "tie":
        so = m.get("super_over_winner")
        return "Match tied" + ("" if _missing(so) else f" ({so} won the super over)")
    return str(m.get("result") or "").capitalize()


def style_ratings(df: pd.DataFrame, column: str = "Rating"):
    """Colour a rating column by band (values formatted to one decimal, "–" if missing)."""
    def colour(v):
        _, c = band(v)
        return f"background-color: {c}; color: white; font-weight: 600; text-align: center"
    return df.style.map(colour, subset=[column]).format({column: fmt_rating})


def rating_chart(df: pd.DataFrame, title: Optional[str] = None) -> alt.Chart:
    """Bar chart of ratings, one evenly spaced bar per match, oldest first.
    `df` needs match_date, rating and label (shown in the tooltip). Bars rise from 3,
    the bottom of the scale."""
    d = df.sort_values("match_date").reset_index(drop=True).copy()
    d["band"] = d["rating"].map(lambda r: band(r)[0])
    d["slot"] = range(len(d))
    d["date_text"] = d["match_date"].dt.strftime("%d %b %y")
    order = [label for _, label, _ in BANDS]
    colours = alt.Scale(domain=order, range=[c for _, _, c in BANDS])
    labels = dict(zip(d["slot"], d["date_text"]))
    label_expr = " : ".join(f"datum.value == {k} ? '{v}'" for k, v in labels.items()) + " : ''"
    chart = alt.Chart(d).mark_bar().encode(
        x=alt.X("slot:O", title=None, axis=alt.Axis(labelExpr=label_expr, labelAngle=-45)),
        y=alt.Y("rating:Q", scale=alt.Scale(domain=[3, 10]), title="Rating"),
        y2=alt.datum(3),
        color=alt.Color("band:N", scale=colours, legend=alt.Legend(title=None, orient="bottom")),
        tooltip=[alt.Tooltip("date_text:N", title="Date"), alt.Tooltip("label:N", title="Match"),
                 alt.Tooltip("rating:Q", title="Rating", format=".1f")],
    ).properties(height=280)
    return chart.properties(title=title) if title else chart
