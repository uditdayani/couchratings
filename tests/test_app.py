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
    assert ui.fielding_line(2, 0, 1) == "2 ct, 1 ro"
    assert ui.score_line(187, 4, 120) == "187/4 (20.0 ov)"
    assert ui.score_line(143, 10, 112) == "143 (18.4 ov)"        # all out


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
    log = at.dataframe[0].value
    assert {"Date", "Rating", "Batting", "Bowling"} <= set(log.columns)


def test_match_page(demo):
    mid = demo["matches"]["match_id"].iloc[5]
    m = demo["matches"].iloc[5]
    at = run("views/match.py", match=mid)
    assert at.title[0].value == f"{m['team_1']} v {m['team_2']}"
    assert len(at.dataframe) == 2                                 # one card per team
    assert len(at.dataframe[0].value) == 11


def test_about_page(demo):
    at = run("views/about.py")
    assert at.title[0].value == "About these ratings"
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
