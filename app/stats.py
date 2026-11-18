"""Calculations behind the dashboard pages, in plain pandas so they can be tested on their own.

`ratings` is the ratings dataset (one row per player per match, `involved` marks a rating),
`matches` the matches dataset and `matchups` the batter-vs-bowler dataset (see data.py).
"""
from typing import Iterable, Optional

import numpy as np
import pandas as pd

GOOD_DAY = 7.0       # a rating this high or higher counts as a good day
OFF_DAY = 5.5        # this low or lower counts as an off day


def rated(ratings: pd.DataFrame) -> pd.DataFrame:
    return ratings[ratings["involved"] & ratings["rating"].notna()]


# ------------------------------------------------------------------ player: teams

def teams_played_for(ratings: pd.DataFrame, player_id: str) -> pd.DataFrame:
    """Every team a player has played for, most recent first: competition, matches, years."""
    mine = ratings[ratings["player_id"] == player_id]
    if mine.empty:
        return pd.DataFrame(columns=["Team", "Competition", "Matches", "Years"])
    g = (mine.groupby(["team", "competition"])
         .agg(matches=("match_id", "nunique"), first=("match_date", "min"), last=("match_date", "max"))
         .reset_index().sort_values("last", ascending=False))
    years = [f"{a.year}" if a.year == b.year else f"{a.year}–{b.year}" for a, b in zip(g["first"], g["last"])]
    return pd.DataFrame({"Team": g["team"].values, "Competition": g["competition"].values,
                         "Matches": g["matches"].values, "Years": years})


# ------------------------------------------------------------------ player: consistency

def consistency(ratings: pd.DataFrame) -> pd.DataFrame:
    """Per player: rated matches, average, median, spread (standard deviation), and the share
    of good days (7.0+) and off days (5.5 or less)."""
    r = rated(ratings)
    g = r.groupby("player_id")["rating"]
    out = pd.DataFrame({
        "matches": g.size(),
        "average": g.mean(),
        "median": g.median(),
        "spread": g.std(ddof=0),
        "pct_good": g.apply(lambda x: 100 * (x >= GOOD_DAY).mean()),
        "pct_off": g.apply(lambda x: 100 * (x <= OFF_DAY).mean()),
    })
    names = r.drop_duplicates("player_id").set_index("player_id")["name"]
    return out.join(names).round({"average": 2, "median": 2, "spread": 2, "pct_good": 1, "pct_off": 1})


# ------------------------------------------------------------------ player: match history

def filter_history(log: pd.DataFrame, competitions: Optional[Iterable[str]] = None,
                   years: Optional[Iterable[int]] = None, role: str = "All",
                   rated_only: bool = False) -> pd.DataFrame:
    """A player's matches, newest first, filtered. `role`: All, Batted, Bowled."""
    out = log
    if competitions:
        out = out[out["competition"].isin(list(competitions))]
    if years:
        out = out[out["match_date"].dt.year.isin(list(years))]
    if role == "Batted":
        out = out[(out["balls_faced"] > 0) | (out["dismissed"] > 0)]
    elif role == "Bowled":
        out = out[out["balls_bowled"] > 0]
    if rated_only:
        out = out[out["involved"] & out["rating"].notna()]
    return out.sort_values("match_date", ascending=False)


def page_of(df: pd.DataFrame, page: int, size: int) -> pd.DataFrame:
    """Rows for a 1-based page number (clamped to the pages that exist)."""
    pages = max(1, -(-len(df) // size))
    page = min(max(1, int(page)), pages)
    return df.iloc[(page - 1) * size: page * size]


def n_pages(n_rows: int, size: int) -> int:
    return max(1, -(-n_rows // size))


# ------------------------------------------------------------------ records

def best_performances(ratings: pd.DataFrame, competition: Optional[str] = None, n: int = 50) -> pd.DataFrame:
    """Highest single-match ratings; ties broken by impact in runs."""
    r = rated(ratings)
    if competition:
        r = r[r["competition"] == competition]
    return r.sort_values(["rating", "total_impact"], ascending=False).head(n)


def most_nine_plus(ratings: pd.DataFrame, competition: Optional[str] = None, n: int = 25) -> pd.DataFrame:
    r = rated(ratings)
    if competition:
        r = r[r["competition"] == competition]
    g = (r.assign(nine=r["rating"] >= 9.0).groupby(["player_id", "name"])
         .agg(nine_plus=("nine", "sum"), matches=("match_id", "nunique")).reset_index())
    g = g[g["nine_plus"] > 0]
    g["pct"] = (100 * g["nine_plus"] / g["matches"]).round(1)
    return g.sort_values(["nine_plus", "pct"], ascending=False).head(n)


def best_averages(ratings: pd.DataFrame, competition: Optional[str] = None, min_matches: int = 30,
                  n: int = 25) -> pd.DataFrame:
    r = rated(ratings)
    if competition:
        r = r[r["competition"] == competition]
    g = (r.groupby(["player_id", "name"]).agg(average=("rating", "mean"), matches=("match_id", "nunique"))
         .reset_index())
    g = g[g["matches"] >= min_matches]
    return g.assign(average=g["average"].round(2)).sort_values("average", ascending=False).head(n)


def best_seasons(ratings: pd.DataFrame, competition: Optional[str] = None, min_matches: int = 8,
                 n: int = 25) -> pd.DataFrame:
    """Highest average rating by a player in one calendar year of one competition."""
    r = rated(ratings)
    if competition:
        r = r[r["competition"] == competition]
    r = r.assign(year=r["match_date"].dt.year)
    g = (r.groupby(["player_id", "name", "competition", "year"])
         .agg(average=("rating", "mean"), matches=("match_id", "nunique")).reset_index())
    g = g[g["matches"] >= min_matches]
    return g.assign(average=g["average"].round(2)).sort_values("average", ascending=False).head(n)


def most_consistent(ratings: pd.DataFrame, min_matches: int = 30, n: int = 25) -> pd.DataFrame:
    """Smallest spread among players who are also good on average (above the 6.5 median)."""
    c = consistency(ratings).reset_index()
    c = c[(c["matches"] >= min_matches) & (c["average"] >= 6.5)]
    return c.sort_values(["spread", "average"], ascending=[True, False]).head(n)


# ------------------------------------------------------------------ head-to-head

def matchup(matchups: pd.DataFrame, batter_id: str, bowler_id: str) -> pd.DataFrame:
    """One batter against one bowler: a row per competition plus an "All" row."""
    m = matchups[(matchups["batter_id"] == batter_id) & (matchups["bowler_id"] == bowler_id)]
    if m.empty:
        return m
    total = m.sum(numeric_only=True).to_frame().T.assign(competition="All")
    out = pd.concat([m, total], ignore_index=True)
    return add_rates(out)


def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Strike rate, average, dot-ball %, balls per dismissal, and runs vs par per 6 balls."""
    balls = df["balls"].replace(0, np.nan)
    outs = df["dismissals"].replace(0, np.nan)
    return df.assign(
        strike_rate=(100 * df["runs"] / balls).round(1),
        average=(df["runs"] / outs).round(1),
        dot_pct=(100 * df["dots"] / balls).round(1),
        balls_per_dismissal=(df["balls"] / outs).round(1),
        vs_par_per_over=(6 * df["runs_vs_par"] / balls).round(2),
    )


COLS = ["balls", "runs", "dismissals", "dots", "fours", "sixes", "runs_vs_par", "matches"]
EDGE = 0.5   # runs vs par per over: a gap bigger than this is an "upper hand"


def record(rows: pd.DataFrame) -> pd.Series:
    """Totals and rates for a set of matchup rows, as one Series."""
    total = rows[COLS].sum().to_frame().T
    return add_rates(total).iloc[0]


def context(matchups: pd.DataFrame, batter_id: str, bowler_id: str) -> dict:
    """The pairing, plus how other batters fared against this bowler and how this batter
    fared against other bowlers, so a head-to-head is judged against the right yardstick:
    a batter at par against an elite bowler has done well."""
    pair = (matchups["batter_id"] == batter_id) & (matchups["bowler_id"] == bowler_id)
    return {"pair": record(matchups[pair]),
            "others_v_bowler": record(matchups[(matchups["bowler_id"] == bowler_id) & ~pair]),
            "batter_v_others": record(matchups[(matchups["batter_id"] == batter_id) & ~pair])}


def verdict(ctx: dict, min_balls: int = 12) -> str:
    """Who has had the upper hand: the batter's runs vs par per over against this bowler,
    compared with what every other batter has managed against him."""
    pair, others = ctx["pair"], ctx["others_v_bowler"]
    if pair["balls"] < min_balls:
        return "too few"
    if others["balls"] < 60 or pd.isna(others["vs_par_per_over"]):
        base = 0.0                                   # little else to compare with: use par
    else:
        base = others["vs_par_per_over"]
    gap = pair["vs_par_per_over"] - base
    return "batter" if gap > EDGE else "bowler" if gap < -EDGE else "even"


def opponents(matchups: pd.DataFrame, player_id: str, as_role: str = "batter",
              min_balls: int = 12) -> pd.DataFrame:
    """Everyone a batter has faced (or a bowler has bowled to), all competitions combined,
    with at least `min_balls` balls between them. `vs_par_per_over` is from the batter's side:
    positive means the batter came out on top."""
    key, other = ("batter_id", "bowler_id") if as_role == "batter" else ("bowler_id", "batter_id")
    mine = matchups[matchups[key] == player_id]
    cols = ["balls", "runs", "dismissals", "dots", "fours", "sixes", "runs_vs_par", "matches"]
    g = mine.groupby(other)[cols].sum().reset_index().rename(columns={other: "opponent_id"})
    return add_rates(g[g["balls"] >= min_balls])


# ------------------------------------------------------------------ venues

def venue_key(matches: pd.DataFrame) -> pd.Series:
    """Venue id from the Cricsheet register when known, else the venue name as written."""
    vid = matches["venue_id"] if "venue_id" in matches.columns else pd.Series(None, index=matches.index)
    return vid.fillna(matches["venue"])


def venue_names(matches: pd.DataFrame) -> pd.Series:
    """venue key -> display name (the register's unique name, else the name as written)."""
    name = matches["venue_name"] if "venue_name" in matches.columns else matches["venue"]
    return (matches.assign(_k=venue_key(matches), _n=name.fillna(matches["venue"]))
            .drop_duplicates("_k").set_index("_k")["_n"])


def venue_summary(matches: pd.DataFrame) -> pd.DataFrame:
    """Per venue: matches, average first- and second-innings score (full 20-over first innings
    only, so rain doesn't drag it down), and how often the side batting first won."""
    m = matches.assign(_k=venue_key(matches))
    full_first = m["inn1_balls"].fillna(0) >= 120
    decided = m["result"] == "win"
    first_won = decided & (m["winner"] == m["inn1_team"])
    g = m.assign(_full=full_first, _r1=m["inn1_runs"].where(full_first), _r2=m["inn2_runs"],
                 _dec=decided, _fw=first_won).groupby("_k")
    out = pd.DataFrame({
        "matches": g.size(),
        "avg_first_innings": g["_r1"].mean().round(0),
        "avg_second_innings": g["_r2"].mean().round(0),
        "highest_total": g[["inn1_runs", "inn2_runs"]].max().max(axis=1),
        "pct_won_batting_first": (100 * g["_fw"].sum() / g["_dec"].sum().replace(0, np.nan)).round(0),
        "first_match": g["match_date"].min(),
        "last_match": g["match_date"].max(),
    })
    out["name"] = venue_names(matches)
    return out.sort_values("matches", ascending=False)


def with_venue(ratings: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    keys = matches.assign(venue_key=venue_key(matches))[["match_id", "venue_key"]]
    return ratings.merge(keys, on="match_id", how="left")


def venue_players(ratings: pd.DataFrame, matches: pd.DataFrame, venue: str,
                  min_matches: int = 3) -> pd.DataFrame:
    """Players with `min_matches`+ rated matches at a venue: average there, best there,
    and the difference from their average everywhere else."""
    r = with_venue(rated(ratings), matches)
    here = r[r["venue_key"] == venue]
    g = (here.groupby(["player_id", "name"])
         .agg(matches=("match_id", "nunique"), average=("rating", "mean"), best=("rating", "max"))
         .reset_index())
    g = g[g["matches"] >= min_matches]
    elsewhere = r[r["venue_key"] != venue].groupby("player_id")["rating"].mean().rename("elsewhere")
    g = g.merge(elsewhere, on="player_id", how="left")
    g["difference"] = (g["average"] - g["elsewhere"]).round(2)
    return g.assign(average=g["average"].round(2), elsewhere=g["elsewhere"].round(2)) \
            .sort_values("average", ascending=False)


def player_venues(ratings: pd.DataFrame, matches: pd.DataFrame, player_id: str,
                  min_matches: int = 3) -> pd.DataFrame:
    """A player's venues with `min_matches`+ rated matches, best average first, with the
    difference from their own average everywhere else."""
    r = with_venue(rated(ratings[ratings["player_id"] == player_id]), matches)
    if r.empty:
        return pd.DataFrame(columns=["venue_key", "matches", "average", "difference"])
    overall = r["rating"].mean()
    g = r.groupby("venue_key").agg(matches=("match_id", "nunique"), average=("rating", "mean"),
                                   total=("rating", "sum")).reset_index()
    g = g[g["matches"] >= min_matches]
    n = len(r)
    # Compare with the player's average at every other ground, not with an average that
    # includes this ground.
    other = (r["rating"].sum() - g["total"]) / (n - g["matches"]).replace(0, np.nan)
    g["difference"] = (g["average"] - other.fillna(overall)).round(2)
    return g.drop(columns="total").assign(average=g["average"].round(2)) \
            .sort_values("average", ascending=False)


# ------------------------------------------------------------------ big matches

BIG_STAGE = r"final|semi|qualifier|eliminator|play-?off|knock"   # "Final", "Qualifier 1", ...


def is_big(matches: pd.DataFrame) -> pd.Series:
    """Knockout matches: finals, semi-finals, qualifiers, eliminators, play-offs."""
    stage = matches["event_stage"] if "event_stage" in matches.columns else pd.Series(None, index=matches.index)
    return stage.fillna("").str.contains(BIG_STAGE, case=False, regex=True)


def big_match_ids(matches: pd.DataFrame) -> set:
    return set(matches.loc[is_big(matches), "match_id"])


def big_match_players(ratings: pd.DataFrame, matches: pd.DataFrame, competition: Optional[str] = None,
                      min_matches: int = 3) -> pd.DataFrame:
    """Players with `min_matches`+ rated knockout matches: their average there, their best,
    and the difference from their average in other matches."""
    r = rated(ratings)
    if competition:
        r = r[r["competition"] == competition]
    big = r["match_id"].isin(big_match_ids(matches))
    g = (r[big].groupby(["player_id", "name"])
         .agg(matches=("match_id", "nunique"), average=("rating", "mean"), best=("rating", "max"))
         .reset_index())
    g = g[g["matches"] >= min_matches]
    other = r[~big].groupby("player_id")["rating"].mean().rename("other")
    g = g.merge(other, on="player_id", how="left")
    g["difference"] = (g["average"] - g["other"]).round(2)
    return g.assign(average=g["average"].round(2), other=g["other"].round(2)) \
            .sort_values("average", ascending=False)


def player_big_matches(ratings: pd.DataFrame, matches: pd.DataFrame, player_id: str) -> dict:
    """A player's knockout record: matches, average, and difference from his other matches."""
    r = rated(ratings[ratings["player_id"] == player_id])
    big = r["match_id"].isin(big_match_ids(matches))
    if not big.any():
        return {"matches": 0, "average": None, "difference": None}
    other = r.loc[~big, "rating"].mean()
    avg = r.loc[big, "rating"].mean()
    return {"matches": int(big.sum()), "average": round(avg, 2),
            "difference": None if pd.isna(other) else round(avg - other, 2)}


# ------------------------------------------------------------------ team of the season

XI = {"wicketkeeper": 1, "batter": 4, "all-rounder": 2, "bowler": 4}
STAGE_ORDER = {"opener": 0, "top order": 1, "middle order": 2, "finisher": 3}
SHRINK = 3   # matches of an average (6.5) rating added to every player's record


def season_players(ratings: pd.DataFrame, competition: str, year: int, min_matches: int = 6) -> pd.DataFrame:
    """Everyone with `min_matches`+ rated matches in one year of one competition, with a role
    and a selection score: their average shrunk towards 6.5 by 3 matches, so a short great
    run doesn't beat a long very good one."""
    r = rated(ratings)
    r = r[(r["competition"] == competition) & (r["match_date"].dt.year == year)]
    if r.empty:
        return pd.DataFrame()
    r = r.assign(byes=r["byes_conceded"] if "byes_conceded" in r.columns else 0,
                 batting_stage=r["batting_stage"] if "batting_stage" in r.columns else None)
    g = r.groupby(["player_id", "name"]).agg(
        matches=("match_id", "nunique"), average=("rating", "mean"), total=("rating", "sum"),
        best=("rating", "max"), faced=("balls_faced", "sum"), bowled=("balls_bowled", "sum"),
        stumpings=("stumpings", "sum"), byes=("byes", "sum"),
        team=("team", lambda x: x.mode().iloc[0]),
        stage=("batting_stage", lambda x: x.dropna().mode().iloc[0] if x.notna().any() else None),
    ).reset_index()
    g = g[g["matches"] >= min_matches]
    faced_pm, bowled_pm = g["faced"] / g["matches"], g["bowled"] / g["matches"]
    keeper = (g["stumpings"] > 0) | (g["byes"] > 0)
    g["role"] = np.select(
        [keeper, (bowled_pm >= 12) & (faced_pm >= 10), bowled_pm >= 12],
        ["wicketkeeper", "all-rounder", "bowler"], default="batter")
    g["score"] = (g["total"] + 6.5 * SHRINK) / (g["matches"] + SHRINK)
    return g.sort_values("score", ascending=False)


def team_of_season(ratings: pd.DataFrame, competition: str, year: int, min_matches: int = 6) -> pd.DataFrame:
    """The best XI of a season: 1 wicketkeeper, 4 batters, 2 all-rounders and 4 bowlers, each
    the highest-scoring in their role. If a role is short, the best remaining players of any
    role fill the gaps. Listed in batting order: keeper and batters by batting stage, then
    all-rounders, then bowlers."""
    pool = season_players(ratings, competition, year, min_matches)
    if pool.empty:
        return pool
    picked = []
    for role, n in XI.items():
        picked += pool[pool["role"] == role].head(n)["player_id"].tolist()
    if len(picked) < 11:
        rest = pool[~pool["player_id"].isin(picked)]
        picked += rest.head(11 - len(picked))["player_id"].tolist()
    xi = pool[pool["player_id"].isin(picked)].copy()
    order = {"wicketkeeper": 0, "batter": 0, "all-rounder": 1, "bowler": 2}
    xi["_group"] = xi["role"].map(order)
    xi["_stage"] = xi["stage"].map(STAGE_ORDER).fillna(4)
    xi = xi.sort_values(["_group", "_stage", "score"], ascending=[True, True, False])
    return xi.drop(columns=["_group", "_stage", "total"]).assign(
        average=xi["average"].round(2), score=xi["score"].round(2)).reset_index(drop=True)


# ------------------------------------------------------------------ momentum

def momentum(balls: pd.DataFrame) -> pd.DataFrame:
    """Ball by ball, where each innings was heading: projected total = runs so far + the
    runs a typical side would still add from that point (balls left, wickets down, adjusted
    for competition, season and ground). Adds the over.ball label and the score."""
    if balls.empty:
        return balls
    b = balls.sort_values(["innings_no", "seq"]).copy()
    g = b.groupby("innings_no")
    b["runs_after"] = g["runs_total"].cumsum()
    b["runs_before"] = b["runs_after"] - b["runs_total"]
    b["wickets_after"] = g["dismissals"].cumsum()
    b["legal_no"] = g["is_legal"].cumsum()
    # A wide or no-ball shows as the ball it preceded (e.g. 5.3 wide, then 5.3)
    shown = b["legal_no"] + (~b["is_legal"]).astype(int)
    b["over_ball"] = ((shown - 1) // 6).astype(str) + "." + ((shown - 1) % 6 + 1).astype(str)
    b["overs"] = b["legal_no"] / 6
    b["projected_before"] = (b["runs_before"] + b["expected_before"]).round(0)
    b["projected"] = (b["runs_after"] + b["expected_after"]).round(0)
    return b


def swing_balls(m: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """The balls that moved the projected total most, either way."""
    if m.empty:
        return m
    return m.reindex(m["runs_added"].abs().sort_values(ascending=False).index).head(n)


def describe_ball(row, names: pd.Series) -> str:
    """'Bumrah to Kohli: OUT, caught' / 'Russell hits a six off Bumrah' style text."""
    bowler, batter = names.get(row["bowler_id"], "?"), names.get(row["batter_id"], "?")
    if row["dismissals"] > 0:
        out = names.get(row["out_id"], batter) if pd.notna(row.get("out_id")) else batter
        return f"{bowler} to {batter}: **{out} out**, {row['wicket_kind']}"
    if row["runs_batter"] == 6:
        return f"{batter} hits a **six** off {bowler}"
    if row["runs_batter"] == 4:
        return f"{batter} hits a **four** off {bowler}"
    return f"{bowler} to {batter}: {int(row['runs_total'])} run{'s' if row['runs_total'] != 1 else ''}"
