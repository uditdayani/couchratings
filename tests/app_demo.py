"""Synthetic demo data for the dashboard, in the same shape as app/data.py's queries.

Used by the app tests, and to run the dashboard locally without Databricks:
    python tests/app_demo.py demo_data
    CRICKET_RATINGS_DATA_DIR=demo_data streamlit run app/streamlit_app.py
"""
import json
import os
import sys

import numpy as np
import pandas as pd

COMPETITIONS = {
    "Indian Premier League": ["Mumbai Indians", "Chennai Super Kings", "Royal Challengers Bengaluru"],
    "T20I full members": ["India", "Australia", "England"],
    "T20I other": ["Nepal", "Scotland", "Netherlands"],
}


def build(seed: int = 7) -> dict:
    rng = np.random.default_rng(seed)
    squads = {team: [(f"{team[:3].lower()}{i:02d}", f"{team.split()[0][:1]}{i} {team.split()[-1]} Player")
                     for i in range(1, 12)] for teams in COMPETITIONS.values() for team in teams}
    matches, ratings = [], []
    n = 0
    for comp, teams in COMPETITIONS.items():
        for k in range(18):
            n += 1
            t1, t2 = rng.choice(teams, size=2, replace=False)
            date = pd.Timestamp("2025-03-01") + pd.Timedelta(days=int(k * 30 + rng.integers(0, 20)))
            r1, r2 = int(rng.integers(120, 220)), int(rng.integers(110, 220))
            w1, w2 = int(rng.integers(3, 10)), int(rng.integers(3, 11))
            first_wins = r1 > r2
            mid = f"{900000 + n}"
            pom_team = t1 if first_wins else t2
            pom = squads[pom_team][int(rng.integers(0, 4))][0]
            matches.append({
                "match_id": mid, "match_date": date, "season": str(date.year), "event_name": comp,
                "match_number": k + 1, "venue": f"{t1.split()[0]} Stadium", "city": t1.split()[0],
                "team_1": t1, "team_2": t2, "toss_winner": t1, "toss_decision": "bat",
                "result": "win", "winner": t1 if first_wins else t2,
                "win_by_runs": (r1 - r2) if first_wins else None,
                "win_by_wickets": None if first_wins else 10 - w2,
                "method": None, "super_over_winner": None, "player_of_match_ids": [pom],
                "inn1_team": t1, "inn1_runs": r1, "inn1_wickets": w1, "inn1_balls": 120,
                "inn2_team": t2, "inn2_runs": r2 if not first_wins else min(r2, r1 - 1),
                "inn2_wickets": w2, "inn2_balls": int(rng.integers(90, 121)),
            })
            for team in (t1, t2):
                for i, (pid, name) in enumerate(squads[team]):
                    bats, bowls = i < 7, i >= 5
                    faced = int(rng.integers(0, 45)) if bats else int(rng.integers(0, 4))
                    bowled = int(rng.integers(6, 25)) if bowls else 0
                    involved = faced >= 5 or bowled >= 6 or pid == pom
                    impact = float(rng.normal(0, 12)) + (25 if pid == pom else 0)
                    rating = None if not involved else float(np.clip(round(6.5 + impact / 8, 1), 3.0, 10.0))
                    ratings.append({
                        "match_id": mid, "match_date": date, "competition": comp, "team": team,
                        "player_id": pid, "name": name, "involved": involved, "rating": rating,
                        "percentile": None, "total_impact": round(impact, 3),
                        "batting": round(impact * 0.6, 3) if bats else 0.0,
                        "bowling": round(impact * 0.4, 3) if bowls else 0.0, "fielding": 0.0,
                        "runs": int(faced * rng.uniform(0.8, 1.8)), "balls_faced": faced,
                        "dismissed": int(faced > 0 and rng.random() < 0.7),
                        "balls_bowled": bowled, "runs_conceded": int(bowled * rng.uniform(1.0, 1.7)),
                        "wickets": int(rng.integers(0, 4)) if bowled else 0,
                        "catches": int(rng.random() < 0.15), "stumpings": 0, "run_outs": 0,
                    })
    ratings = pd.DataFrame(ratings)
    matches = pd.DataFrame(matches)

    # Form, computed the same way as the pipeline (last 10, halving every 5).
    rated = ratings[ratings["involved"]]
    rows = []
    for scope, part in [("All", rated)] + [(c, rated[rated["competition"] == c]) for c in COMPETITIONS]:
        for pid, g in part.groupby("player_id"):
            g = g.sort_values("match_date", ascending=False).head(10)
            if len(g) < 3:
                continue
            w = 0.5 ** (np.arange(len(g)) / 5)
            rows.append({"scope": scope, "player_id": pid, "name": g["name"].iloc[0],
                         "form_rating": round(float((w * g["rating"]).sum() / w.sum()), 2),
                         "matches_counted": len(g), "last_match_date": g["match_date"].max(),
                         "active": True})
    form = pd.DataFrame(rows)
    form["leaderboard_rank"] = None
    for scope, idx in form[form["matches_counted"] >= 5].groupby("scope").groups.items():
        ordered = form.loc[idx].sort_values("form_rating", ascending=False).index
        form.loc[ordered, "leaderboard_rank"] = range(1, len(ordered) + 1)
    form["leaderboard_rank"] = form["leaderboard_rank"].astype("float")

    meta = pd.DataFrame([{
        "version": "v2", "created_at": pd.Timestamp("2026-11-11 23:30"), "fitted_matches": 3800,
        "first_match_date": pd.Timestamp("2019-01-01"), "fitted_to": pd.Timestamp("2026-11-01"),
        "params": json.dumps({"min_date": "2019-01-01"}), "notes": "demo",
        "latest_match": matches["match_date"].max(), "last_ingest": pd.Timestamp("2026-11-12 06:00")}])
    validation = pd.DataFrame([
        {"check": "calibration", "scope": "Indian Premier League", "value": 0.22,
         "target": "|avg| <= 3.0 runs", "passed": True, "detail": "541 first innings since 2019",
         "checked_at": pd.Timestamp("2026-11-13 12:00")},
        {"check": "player_of_match_top3", "scope": "All", "value": 81.6, "target": ">= 70%",
         "passed": True, "detail": "4564 matches", "checked_at": pd.Timestamp("2026-11-13 12:00")},
    ])
    return {"ratings": ratings, "matches": matches, "form": form, "meta": meta, "validation": validation}


def write(folder: str, seed: int = 7) -> dict:
    os.makedirs(folder, exist_ok=True)
    tables = build(seed)
    for name, df in tables.items():
        df.to_parquet(os.path.join(folder, f"{name}.parquet"), index=False)
    return tables


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "demo_data"
    write(out)
    print(f"Demo data written to {out}/")
