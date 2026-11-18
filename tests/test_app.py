"""Dashboard: formatting helpers, and every page rendering on demo data."""
import math
import os
import sys

import pandas as pd
import pytest

APP = os.path.join(os.path.dirname(__file__), "..", "app")
sys.path.insert(0, APP)

import ui  # noqa: E402
from app_demo import write  # noqa: E402

streamlit_testing = pytest.importorskip("streamlit.testing.v1")


# ------------------------------------------------------------------ formatting

def test_rating_bands():
    assert ui.band(9.4)[0] == "Exceptional" and ui.band(9.0)[0] == "Exceptional"
    assert ui.band(6.5)[0] == "Good" and ui.band(6.4)[0] == "Average" and ui.band(3.0)[0] == "Poor"
    assert ui.band(None)[0] == "Not rated" and ui.band(math.nan)[0] == "Not rated"
    assert ui.fmt_rating(None) == "–" and ui.fmt_rating(7.25) == "7.2" or ui.fmt_rating(7.25) == "7.3"


def test_scorecard_lines():
    assert ui.batting_line(45, 30, 1) == "45 (30)"
    assert ui.batting_line(45, 30, 0) == "45* (30)"
    assert ui.batting_line(0, 0, 0) == ""
    assert ui.batting_line(0, 0, 1) == "0 (0)"                    # run out without facing
    assert ui.bowling_line(2, 24, 24) == "2/24 (4.0)"
    assert ui.bowling_line(1, 30, 22) == "1/30 (3.4)"
    assert ui.bowling_line(0, 0, 0) == ""
    assert ui.bowling_with_par(1, 34, 24, 31.2, 1.18) == "1/34 (4.0) · par 1.2/31"
    assert ui.bowling_with_par(1, 34, 24, None, None) == "1/34 (4.0)"     # built before par existed
    assert ui.bowling_with_par(0, 0, 0, None, None) == ""
    assert ui.fielding_line(2, 0, 1) == "2 ct, 1 ro"
    assert ui.score_line(187, 4, 120) == "187/4 (20.0 ov)"
    assert ui.score_line(143, 10, 112) == "143 (18.4 ov)"        # all out


def test_player_teams_prefers_nationality_over_exhibition_sides():
    df = pd.DataFrame({
        "player_id": ["mm"] * 3 + ["ip"] + ["sw"] * 2,
        "match_date": pd.to_datetime(["2016-01-01", "2017-05-01", "2018-05-31", "2024-04-01",
                                      "2015-01-01", "2020-01-01"]),
        "competition": ["T20I full members", "Indian Premier League", "T20I other",
                        "Indian Premier League", "T20I other", "T20I other"],
        "team": ["South Africa", "Delhi Daredevils", "ICC World XI", "Mumbai Indians",
                 "Ireland", "England"],
    })
    t = ui.player_teams(df)
    assert t.loc["mm", "label"] == "South Africa" and t.loc["mm", "club"] == "Delhi Daredevils"
    assert t.loc["ip", "label"] == "Mumbai Indians"                 # never played a T20I
    assert t.loc["sw", "label"] == "England"                         # most recent country


def test_result_lines():
    base = {"method": None, "super_over_winner": None}
    assert ui.result_line({**base, "result": "win", "winner": "India", "win_by_runs": 11,
                           "win_by_wickets": None}) == "India won by 11 runs"
    assert ui.result_line({**base, "result": "win", "winner": "Nepal", "win_by_runs": None,
                           "win_by_wickets": 1, "method": "D/L"}) == "Nepal won by 1 wicket (D/L)"
    assert ui.result_line({**base, "result": "tie", "super_over_winner": "Beta"}) == \
        "Match tied (Beta won the super over)"


# ------------------------------------------------------------------- pages

@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    folder = tmp_path_factory.mktemp("demo")
    tables = write(str(folder))
    os.environ["CRICKET_RATINGS_DATA_DIR"] = str(folder)
    yield tables
    os.environ.pop("CRICKET_RATINGS_DATA_DIR", None)


def run(page=None, **query):
    at = streamlit_testing.AppTest.from_file(os.path.join(APP, "streamlit_app.py"), default_timeout=60)
    for k, v in query.items():
        at.query_params[k] = v
    at.run()
    if page:
        at.switch_page(page).run()
    assert not at.exception, at.exception
    return at


def test_leaderboard_is_the_home_page(demo):
    at = run()
    assert at.title[0].value == "Form leaderboard"
    board = at.dataframe[0].value
    assert len(board) > 0 and board.index.size == board["Rank"].nunique()


def test_leaderboard_filters_by_competition(demo):
    at = run()
    at.selectbox[0].set_value("T20I other").run()
    assert not at.exception
    names = set(at.dataframe[0].value["Player"])
    nepal_etc = set(demo["ratings"][demo["ratings"]["competition"] == "T20I other"]["name"])
    assert names <= nepal_etc


def test_player_page(demo):
    pid = demo["form"].query("scope == 'All'").sort_values("form_rating").iloc[-1]["player_id"]
    name = demo["ratings"].query("player_id == @pid")["name"].iloc[0]
    at = run("views/player.py", player=pid)
    assert at.title[0].value == name
    assert any(m.label == "Current form" for m in at.metric)
    log = at.dataframe[-1].value
    assert {"Date", "Rating", "Batting", "Bowling"} <= set(log.columns)


def test_match_page(demo):
    mid = demo["matches"]["match_id"].iloc[5]
    m = demo["matches"].iloc[5]
    at = run("views/match.py", match=mid)
    assert at.title[0].value == f"{m['team_1']} v {m['team_2']}"
    assert len(at.dataframe) >= 2                                 # one card per team
    assert len(at.dataframe[0].value) == 11
    assert at.dataframe[0].value["Bowling"].str.contains("par").any()


def test_player_page_sections(demo):
    pid = demo["ratings"]["player_id"].value_counts().index[0]
    at = run("views/player.py", player=pid)
    labels = {m.label for m in at.metric}
    assert {"Median rating", "Spread", "Good days", "Off days"} <= labels
    subheads = {s.value for s in at.subheader}
    assert {"Teams played for", "Best grounds", "Head-to-head", "Match history"} <= subheads
    history = at.dataframe[-1].value
    assert "Team" in history.columns and len(history) <= 25


def test_player_history_filters(demo):
    pid = demo["ratings"]["player_id"].value_counts().index[0]
    at = run("views/player.py", player=pid)
    comp = demo["ratings"].query("player_id == @pid")["competition"].iloc[0]
    at.multiselect(key="hist_comp").set_value([comp]).run()
    assert not at.exception
    assert set(at.dataframe[-1].value["Competition"]) == {comp}


def test_records_page(demo):
    at = run("views/records.py")
    assert at.title[0].value == "Records"
    best = at.dataframe[0].value
    assert len(best) == 50 and best["Rating"].iloc[0] == demo["ratings"]["rating"].max()


def test_head_to_head_page(demo):
    pair = demo["matchups"].groupby(["batter_id", "bowler_id"])["balls"].sum().idxmax()
    at = run("views/head_to_head.py", batter=pair[0], bowler=pair[1])
    assert at.title[0].value == "Head-to-head"
    table = at.dataframe[0].value
    assert "All" in set(table["Competition"])
    total = demo["matchups"].query("batter_id == @pair[0] and bowler_id == @pair[1]")["balls"].sum()
    assert table.set_index("Competition").loc["All", "Balls"] == total


def test_head_to_head_without_data_explains(demo, tmp_path):
    import shutil
    for f in os.listdir(os.environ["CRICKET_RATINGS_DATA_DIR"]):
        if f != "matchups.parquet":
            shutil.copy(os.path.join(os.environ["CRICKET_RATINGS_DATA_DIR"], f), tmp_path / f)
    old = os.environ["CRICKET_RATINGS_DATA_DIR"]
    os.environ["CRICKET_RATINGS_DATA_DIR"] = str(tmp_path)
    try:
        at = run("views/head_to_head.py")
        assert "isn't available yet" in at.info[0].value
    finally:
        os.environ["CRICKET_RATINGS_DATA_DIR"] = old


def test_venues_page(demo):
    at = run("views/venues.py")
    assert at.title[0].value == "Venues"
    assert any(m.label == "Average first innings" for m in at.metric)
    assert at.subheader[0].value.endswith("Stadium, " + at.subheader[0].value.split()[0])   # register name


def test_team_of_the_season_page(demo):
    at = run("views/team_of_season.py")
    assert at.title[0].value == "Team of the season"
    at.number_input[0].set_value(2).run()
    assert not at.exception
    xi = at.dataframe[0].value
    assert len(xi) == 11 and list(xi["#"]) == list(range(1, 12))


def test_records_big_match_tab(demo):
    at = run("views/records.py")
    assert not at.exception
    assert any("knockout" in c.value for c in at.caption)


def test_match_page_shows_stage(demo):
    final = demo["matches"].query("event_stage == 'Final'")["match_id"].iloc[0]
    at = run("views/match.py", match=final)
    assert any(", Final ·" in c.value and c.value.rstrip().endswith("Stadium, " + c.value.split("Stadium, ")[-1])
               and not c.value.rstrip().endswith("·") for c in at.caption)


def test_match_page_momentum(demo):
    mid = demo["balls"]["match_id"].iloc[0]
    at = run("views/match.py", match=mid)
    heads = [h.value for h in at.subheader]
    assert "Momentum" in heads and "Biggest swing balls" in heads
    swings = at.dataframe[-1].value
    assert len(swings) == 5 and "What happened" in swings.columns


def test_player_photo_with_attribution(demo):
    pid = demo["photos"]["player_id"].iloc[0]
    at = run("views/player.py", player=pid)
    credit = [c.value for c in at.caption if "Wikimedia Commons" in c.value]
    assert credit and "Demo Photographer" in credit[0] and "CC BY-SA 4.0" in credit[0]


def test_about_page(demo):
    at = run("views/about.py")
    assert at.title[0].value == "About Slogonomics"
    assert any(m.label == "Model version" and m.value == "v2" for m in at.metric)


def test_missing_data_shows_an_error_not_a_crash(tmp_path):
    os.environ["CRICKET_RATINGS_DATA_DIR"] = str(tmp_path)        # empty folder
    try:
        at = streamlit_testing.AppTest.from_file(os.path.join(APP, "streamlit_app.py"), default_timeout=60)
        at.run()
        assert not at.exception
        assert "couldn't be loaded" in at.error[0].value
    finally:
        os.environ.pop("CRICKET_RATINGS_DATA_DIR", None)


def test_reads_the_parquet_export_from_a_url(demo, monkeypatch, tmp_path):
    """With [data] url set, datasets are downloaded as Parquet files: no warehouse."""
    import data
    folder = str(tmp_path)
    write(folder)
    old = os.environ.pop("CRICKET_RATINGS_DATA_DIR", None)
    asked = []

    class Resp:
        def __init__(self, path):
            self.content = open(path, "rb").read()

        def raise_for_status(self):
            pass

    def fake_get(url, timeout):
        asked.append(url)
        return Resp(os.path.join(folder, url.rsplit("/", 1)[-1]))

    monkeypatch.setenv("CRICKET_RATINGS_DATA_URL", "https://example.org/releases/download/data-latest/")
    monkeypatch.setattr(data.requests, "get", fake_get)
    try:
        data._load.clear()
        df = data.load("ratings")
        assert asked == ["https://example.org/releases/download/data-latest/ratings.parquet"]
        assert len(df) == len(demo["ratings"])
        balls = data.match_balls(demo["balls"]["match_id"].iloc[0])
        assert len(balls) and balls["match_id"].nunique() == 1
    finally:
        if old:
            os.environ["CRICKET_RATINGS_DATA_DIR"] = old
        data._load.clear()
