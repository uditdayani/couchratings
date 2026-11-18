"""Dashboard calculations (app/stats.py) against small hand-made tables."""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
import stats  # noqa: E402


def perf(pid, rating, date, comp="Cup", team="Alpha", mid=None, name=None, **kw):
    row = {"match_id": mid or f"{pid}-{date}", "player_id": pid, "name": name or pid.upper(),
           "rating": rating, "involved": rating is not None, "match_date": pd.Timestamp(date),
           "competition": comp, "team": team, "total_impact": (rating or 0) * 3,
           "balls_faced": 10, "dismissed": 1, "balls_bowled": 0}
    row.update(kw)
    return row


def frame(rows):
    return pd.DataFrame(rows)


def test_teams_played_for_lists_every_team_newest_first():
    r = frame([perf("a", 7, "2018-04-01", team="Delhi", comp="IPL"),
               perf("a", 7, "2019-04-01", team="Delhi", comp="IPL"),
               perf("a", 6, "2016-01-01", team="South Africa", comp="T20I"),
               perf("a", 6, "2021-05-01", team="Mumbai", comp="IPL")])
    t = stats.teams_played_for(r, "a")
    assert list(t["Team"]) == ["Mumbai", "Delhi", "South Africa"]
    assert t.set_index("Team").loc["Delhi", "Matches"] == 2
    assert t.set_index("Team").loc["Delhi", "Years"] == "2018–2019"
    assert t.set_index("Team").loc["Mumbai", "Years"] == "2021"


def test_consistency_spread_and_good_and_off_days():
    steady = [perf("s", r, f"2024-01-{i + 1:02d}") for i, r in enumerate([6.5, 7.0, 6.5, 7.0])]
    streaky = [perf("x", r, f"2024-01-{i + 1:02d}") for i, r in enumerate([4.0, 9.5, 4.0, 9.5])]
    unrated = [perf("s", None, "2024-02-01")]
    c = stats.consistency(frame(steady + streaky + unrated))
    assert c.loc["s", "matches"] == 4                       # the unrated match doesn't count
    assert c.loc["s", "spread"] == 0.25 and c.loc["x", "spread"] == 2.75
    assert c.loc["s", "pct_good"] == 50.0 and c.loc["s", "pct_off"] == 0.0
    assert c.loc["x", "pct_good"] == 50.0 and c.loc["x", "pct_off"] == 50.0


def test_history_filters_and_pages():
    rows = [perf("a", 6 + (i % 3), f"{2020 + i % 3}-05-{i % 28 + 1:02d}", comp="IPL" if i % 2 else "T20I",
                 balls_faced=0 if i % 4 == 0 else 10, dismissed=0, balls_bowled=24 if i % 4 == 0 else 0)
            for i in range(60)]
    rows.append(perf("a", None, "2023-01-01", comp="IPL"))
    log = frame(rows)
    assert len(stats.filter_history(log)) == 61
    assert set(stats.filter_history(log, ["IPL"])["competition"]) == {"IPL"}
    assert set(stats.filter_history(log, years=[2021])["match_date"].dt.year) == {2021}
    assert (stats.filter_history(log, role="Bowled")["balls_bowled"] > 0).all()
    assert (stats.filter_history(log, role="Batted")["balls_faced"] > 0).all()
    assert stats.filter_history(log, rated_only=True)["rating"].notna().all()
    newest_first = stats.filter_history(log)["match_date"]
    assert newest_first.is_monotonic_decreasing
    assert stats.n_pages(61, 25) == 3 and stats.n_pages(0, 25) == 1
    assert len(stats.page_of(log, 3, 25)) == 11 and len(stats.page_of(log, 99, 25)) == 11   # clamped


def test_records():
    rows = []
    for i in range(40):
        rows.append(perf("great", 7.5 + (i % 2) * 2, f"2024-{i % 12 + 1:02d}-{i % 28 + 1:02d}", mid=f"g{i}"))
        rows.append(perf("solid", 7.0, f"2024-{i % 12 + 1:02d}-{i % 28 + 1:02d}", mid=f"s{i}"))
        rows.append(perf("meh", 6.0, f"2024-{i % 12 + 1:02d}-{i % 28 + 1:02d}", mid=f"m{i}"))
    rows.append(perf("oneoff", 10.0, "2024-06-01", mid="o1"))
    r = frame(rows)
    assert stats.best_performances(r, n=1)["player_id"].iloc[0] == "oneoff"
    most = stats.most_nine_plus(r)
    assert most.iloc[0]["player_id"] == "great" and most.iloc[0]["nine_plus"] == 20
    avg = stats.best_averages(r, min_matches=30)
    assert list(avg["player_id"]) == ["great", "solid", "meh"]           # oneoff: too few matches
    seasons = stats.best_seasons(r, min_matches=8)
    assert seasons.iloc[0]["player_id"] == "great" and seasons.iloc[0]["year"] == 2024
    cons = stats.most_consistent(r, min_matches=30)
    assert list(cons["player_id"]) == ["solid", "great"]                 # meh averages below 6.5


MATCHUP_COLS = ["competition", "batter_id", "bowler_id", "matches", "balls", "runs", "dismissals",
                "dots", "fours", "sixes", "runs_vs_par"]


def test_matchup_totals_and_rates():
    m = pd.DataFrame([["IPL", "bat", "bowl", 3, 30, 45, 1, 10, 4, 2, 6.0],
                      ["T20I", "bat", "bowl", 1, 6, 3, 1, 4, 0, 0, -11.0],
                      ["IPL", "bat", "other", 2, 12, 20, 0, 3, 2, 1, 4.0]], columns=MATCHUP_COLS)
    h = stats.matchup(m, "bat", "bowl").set_index("competition")
    assert h.loc["All", "balls"] == 36 and h.loc["All", "runs"] == 48 and h.loc["All", "dismissals"] == 2
    assert h.loc["All", "strike_rate"] == pytest.approx(133.3, abs=0.05)
    assert h.loc["All", "average"] == 24.0 and h.loc["IPL", "average"] == 45.0
    assert h.loc["T20I", "strike_rate"] == 50.0 and h.loc["T20I", "vs_par_per_over"] == -11.0
    assert h.loc["All", "vs_par_per_over"] == pytest.approx(6 * -5.0 / 36, abs=0.01)
    assert stats.matchup(m, "bat", "nobody").empty


def test_opponents_need_enough_balls():
    m = pd.DataFrame([["IPL", "bat", "b1", 2, 20, 30, 0, 5, 3, 1, 8.0],
                      ["IPL", "bat", "b2", 1, 5, 2, 1, 3, 0, 0, -9.0],
                      ["IPL", "x", "b1", 1, 30, 20, 2, 15, 1, 0, -14.0]], columns=MATCHUP_COLS)
    as_bat = stats.opponents(m, "bat")
    assert list(as_bat["opponent_id"]) == ["b1"]                       # b2: only 5 balls
    as_bowl = stats.opponents(m, "b1", as_role="bowler")
    assert set(as_bowl["opponent_id"]) == {"bat", "x"}


def venue_matches():
    rows = []
    for i in range(6):
        rows.append({"match_id": f"m{i}", "match_date": pd.Timestamp(f"202{i % 4}-04-0{i + 1}"),
                     "venue": "Wankhede Stadium" if i % 2 else "Wankhede Stadium, Mumbai",
                     "venue_id": "w1", "venue_name": "Wankhede Stadium, Mumbai", "city": "Mumbai",
                     "inn1_team": "A", "inn1_runs": 180 + i, "inn1_balls": 120 if i != 5 else 60,
                     "inn2_team": "B", "inn2_runs": 170, "inn2_wickets": 8, "inn1_wickets": 5,
                     "inn2_balls": 120, "result": "win", "winner": "A" if i < 4 else "B",
                     "team_1": "A", "team_2": "B"})
    rows.append({"match_id": "x1", "match_date": pd.Timestamp("2022-01-01"), "venue": "Unlisted Oval",
                 "venue_id": None, "venue_name": None, "city": None, "inn1_team": "A", "inn1_runs": 150,
                 "inn1_balls": 120, "inn2_team": "B", "inn2_runs": 151, "inn2_wickets": 3,
                 "inn1_wickets": 9, "inn2_balls": 100, "result": "win", "winner": "B",
                 "team_1": "A", "team_2": "B"})
    return pd.DataFrame(rows)


def test_venue_summary_merges_name_variants():
    s = stats.venue_summary(venue_matches())
    w = s.loc["w1"]
    assert w["matches"] == 6 and w["name"] == "Wankhede Stadium, Mumbai"   # both spellings, one ground
    assert w["avg_first_innings"] == round(np.mean([180, 181, 182, 183, 184]))   # rain-cut 6th left out
    assert w["pct_won_batting_first"] == round(100 * 4 / 6)
    assert s.loc["Unlisted Oval", "name"] == "Unlisted Oval"                 # not in the register


def test_venue_players_and_player_venues():
    m = venue_matches()
    rows = [perf("gill", 9.0, d, mid=f"m{i}") for i, d in enumerate(m["match_date"][:4])]
    rows += [perf("gill", 6.0, f"2023-0{i + 1}-15", mid=f"e{i}") for i in range(4)]
    rows += [perf("other", 6.5, d, mid=f"m{i}") for i, d in enumerate(m["match_date"][:2])]
    r = frame(rows)
    allm = pd.concat([m, pd.DataFrame([{"match_id": f"e{i}", "venue": "Elsewhere", "venue_id": "e",
                                        "venue_name": "Elsewhere"} for i in range(4)])])
    vp = stats.venue_players(r, allm, "w1", min_matches=3)
    assert list(vp["player_id"]) == ["gill"]                               # other: only 2 here
    assert vp.iloc[0]["difference"] == 3.0
    pv = stats.player_venues(r, allm, "gill").set_index("venue_key")
    assert pv.loc["w1", "average"] == 9.0 and pv.loc["w1", "difference"] == 3.0
    assert pv.loc["e", "difference"] == -3.0


def test_verdict_judges_against_other_batters_facing_the_same_bowler():
    # An elite death bowler: other batters are well below par against him. A batter at
    # roughly par against him (the Dhoni v Bhuvneshwar case) has the upper hand.
    rows = [["IPL", "dhoni", "elite", 12, 60, 102, 0, 17, 6, 6, 4.9]]
    rows += [["IPL", f"b{i}", "elite", 3, 30, 35, 3, 14, 3, 1, -8.0] for i in range(10)]
    rows += [["IPL", "dhoni", f"x{i}", 5, 40, 70, 2, 10, 5, 4, 6.0] for i in range(5)]
    m = pd.DataFrame(rows, columns=MATCHUP_COLS)
    ctx = stats.context(m, "dhoni", "elite")
    assert ctx["pair"]["vs_par_per_over"] == pytest.approx(0.49, abs=0.01)
    assert ctx["others_v_bowler"]["vs_par_per_over"] == pytest.approx(-1.6, abs=0.01)
    assert ctx["others_v_bowler"]["balls_per_dismissal"] == 10
    assert ctx["batter_v_others"]["balls"] == 200
    assert stats.verdict(ctx) == "batter"
    # The same numbers against a bowler others also score freely against: even.
    easy = m.assign(runs_vs_par=np.where(m["bowler_id"] == "elite",
                                         np.where(m["batter_id"] == "dhoni", 4.9, 2.0), m["runs_vs_par"]))
    assert stats.verdict(stats.context(easy, "dhoni", "elite")) == "even"
    assert stats.verdict(stats.context(m.assign(balls=5), "dhoni", "elite")) == "too few"


def test_big_matches():
    m = pd.DataFrame({"match_id": ["f", "q", "l1", "l2", "s8"],
                      "event_stage": ["Final", "Qualifier 1", None, None, "Super 8"]})
    assert stats.big_match_ids(m) == {"f", "q"}                           # Super 8 is a group stage
    rows = [perf("clutch", 9.0, "2024-05-26", mid="f"), perf("clutch", 8.6, "2024-05-21", mid="q"),
            perf("clutch", 6.0, "2024-04-01", mid="l1"), perf("clutch", 6.4, "2024-04-05", mid="l2"),
            perf("other", 5.0, "2024-05-26", mid="f")]
    r = frame(rows)
    b = stats.big_match_players(r, m, min_matches=2)
    assert list(b["player_id"]) == ["clutch"]                              # other: only 1 big match
    assert b.iloc[0]["average"] == 8.8 and b.iloc[0]["difference"] == 2.6
    me = stats.player_big_matches(r, m, "clutch")
    assert me == {"matches": 2, "average": 8.8, "difference": 2.6}
    assert stats.player_big_matches(r, m.assign(event_stage=None), "clutch")["matches"] == 0


def season(pid, n, rating, faced=20, bowled=0, stumpings=0, stage="top order", team="Alpha"):
    return [perf(pid, rating, f"2024-04-{i + 1:02d}", comp="IPL", team=team, mid=f"{pid}{i}",
                 balls_faced=faced, balls_bowled=bowled, stumpings=stumpings, batting_stage=stage)
            for i in range(n)]


def test_team_of_the_season_picks_by_role():
    rows = []
    rows += season("keeper", 10, 7.5, stumpings=1, stage="middle order")
    rows += season("keeper2", 10, 7.9, stumpings=1)                        # better keeper
    for i in range(6):
        rows += season(f"bat{i}", 10, 7.0 + i * 0.2, stage="opener" if i == 5 else "top order")
    for i in range(3):
        rows += season(f"ar{i}", 10, 7.0 + i * 0.2, faced=15, bowled=24, stage="finisher")
    for i in range(6):
        rows += season(f"bowl{i}", 10, 7.0 + i * 0.2, faced=2, bowled=24, stage="finisher")
    rows += season("flash", 2, 10.0)                                       # too few matches
    rows += season("short", 6, 9.0)                                        # 6 matches of 9.0
    xi = stats.team_of_season(frame(rows), "IPL", 2024, min_matches=6)
    assert len(xi) == 11
    assert xi["role"].value_counts().to_dict() == {"batter": 4, "bowler": 4, "all-rounder": 2,
                                                   "wicketkeeper": 1}
    assert "keeper2" in set(xi["player_id"]) and "keeper" not in set(xi["player_id"])
    assert "flash" not in set(xi["player_id"]) and "short" in set(xi["player_id"])
    assert xi.iloc[0]["player_id"] == "bat5"                               # the opener bats first
    assert list(xi["role"].tail(4)) == ["bowler"] * 4
    # (6 x 9.0 + 3 x 6.5) / 9 = 8.17: shrunk towards 6.5 for a short season
    assert xi.set_index("player_id").loc["short", "score"] == pytest.approx(8.17, abs=0.01)


def test_team_of_the_season_fills_a_short_role():
    rows = []
    for i in range(10):
        rows += season(f"bat{i}", 8, 7.0 + i * 0.1)
    rows += season("only_bowler", 8, 7.0, faced=0, bowled=24)
    xi = stats.team_of_season(frame(rows), "IPL", 2024)
    assert len(xi) == 11 and (xi["role"] == "bowler").sum() == 1         # no keeper or all-rounders


def test_momentum_projection_and_swing_balls():
    rows = []
    # Innings 1: 4 off the first ball, a wide, a wicket, then a single.
    for seq, (legal, runs, bat, out, e0, e1) in enumerate([
            (True, 4, 4, 0, 160.0, 158.6), (False, 1, 0, 0, 158.6, 158.6),
            (True, 0, 0, 1, 158.6, 148.0), (True, 1, 1, 0, 148.0, 146.9)], start=1):
        rows.append({"match_id": "m", "innings_no": 1, "seq": seq, "batting_team": "A",
                     "batter_id": "x", "bowler_id": "y", "is_legal": legal, "runs_batter": bat,
                     "runs_total": runs, "dismissals": out, "wicket_kind": "bowled" if out else None,
                     "out_id": "x" if out else None, "expected_before": e0, "expected_after": e1,
                     "runs_added": runs + e1 - e0})
    m = stats.momentum(pd.DataFrame(rows))
    assert list(m["over_ball"]) == ["0.1", "0.2", "0.2", "0.3"]           # the wide shows as 0.2
    assert list(m["projected"]) == [163.0, 164.0, 153.0, 153.0]           # 4 + 158.6, 5 + 158.6, ...
    assert list(m["wickets_after"]) == [0, 0, 1, 1]
    top = stats.swing_balls(m, n=2)
    assert list(top["seq"]) == [3, 1]                                     # the wicket swung most
    names = pd.Series({"x": "Kohli", "y": "Bumrah"})
    assert stats.describe_ball(top.iloc[0], names) == "Bumrah to Kohli: **Kohli out**, bowled"
    assert stats.describe_ball(top.iloc[1], names) == "Kohli hits a **four** off Bumrah"
    assert stats.momentum(pd.DataFrame()).empty
