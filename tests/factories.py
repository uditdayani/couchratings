"""Tiny Cricsheet-format matches for tests (https://cricsheet.org/format/json)."""
import json
import os

REGISTRY = {
    "A1": "a0000001", "A2": "a0000002", "A3": "a0000003",
    "B1": "b0000001", "B2": "b0000002", "B3": "b0000003",
    "Umpire One": "u0000001",  # officials are in the registry too, but are not players
}
PLAYERS = {"Alpha": ["A1", "A2", "A3"], "Beta": ["B1", "B2", "B3"]}


def ball(batter, bowler, non_striker, bat=0, extras=None, wickets=None):
    extra_runs = sum(extras.values()) if extras else 0
    d = {"batter": batter, "bowler": bowler, "non_striker": non_striker,
         "runs": {"batter": bat, "extras": extra_runs, "total": bat + extra_runs}}
    if extras:
        d["extras"] = extras
    if wickets:
        d["wickets"] = wickets
    return d


def match(innings, outcome, match_type="T20", season="2025", player_of_match=None):
    info = {
        "balls_per_over": 6, "city": "Testville", "dates": ["2025-05-01"],
        "event": {"name": "Test League", "match_number": 1},
        "gender": "male", "match_type": match_type, "outcome": outcome, "overs": 20,
        "players": PLAYERS, "registry": {"people": REGISTRY}, "season": season,
        "team_type": "club", "teams": ["Alpha", "Beta"],
        "toss": {"winner": "Alpha", "decision": "bat"}, "venue": "Test Ground",
    }
    if player_of_match:
        info["player_of_match"] = player_of_match
    return {"meta": {"data_version": "1.1.0", "created": "2025-05-02", "revision": 1},
            "info": info, "innings": innings}


def match_a():
    """Alpha 17/2 beat Beta 6/1 by 11 runs. Has a wide, a no-ball, leg-byes,
    a catch and a run out by a substitute."""
    over0 = [
        ball("A1", "B1", "A2", bat=4),                          # legal 1
        ball("A1", "B1", "A2", extras={"wides": 1}),            # wide: still 1 legal
        ball("A1", "B1", "A2", bat=1),                          # legal 2
        ball("A2", "B1", "A1", wickets=[{"kind": "caught", "player_out": "A2",
                                         "fielders": [{"name": "B2"}]}]),  # legal 3
        ball("A3", "B1", "A1", bat=2, extras={"noballs": 1}),   # no-ball: still 3
        ball("A3", "B1", "A1"),                                 # legal 4
        ball("A3", "B1", "A1", extras={"legbyes": 1}),          # legal 5
        ball("A1", "B1", "A3", bat=1),                          # legal 6
    ]
    over1 = [
        ball("A1", "B2", "A3", bat=6),                          # legal 7
        ball("A3", "B2", "A1", wickets=[{"kind": "run out", "player_out": "A3",
                                         "fielders": [{"name": "B3", "substitute": True}]}]),
    ]
    inn2 = [ball("B1", "A1", "B2", bat=2),
            ball("B2", "A1", "B1", wickets=[{"kind": "bowled", "player_out": "B2"}]),
            ball("B1", "A1", "B3", bat=4)]
    return match(
        innings=[{"team": "Alpha", "overs": [{"over": 0, "deliveries": over0},
                                             {"over": 1, "deliveries": over1}]},
                 {"team": "Beta", "target": {"overs": 20, "runs": 18},
                  "overs": [{"over": 0, "deliveries": inn2}]}],
        outcome={"winner": "Alpha", "by": {"runs": 11}},
        player_of_match=["A1"])


def match_b():
    """No result after a few balls. Season given as a number, as some older files do."""
    m = match(innings=[{"team": "Alpha", "overs": [{"over": 0, "deliveries": [
        ball("A1", "B1", "A2", bat=1), ball("A2", "B1", "A1")]}]}],
        outcome={"result": "no result"})
    m["info"]["season"] = 2025
    return m


def match_c():
    """Tie, decided by a super over that Beta won."""
    return match(
        innings=[
            {"team": "Alpha", "overs": [{"over": 0, "deliveries": [ball("A1", "B1", "A2", bat=4)]}]},
            {"team": "Beta", "target": {"overs": 20, "runs": 5},
             "overs": [{"over": 0, "deliveries": [ball("B1", "A1", "B2", bat=4)]}]},
            {"team": "Beta", "super_over": True,
             "overs": [{"over": 0, "deliveries": [ball("B1", "A2", "B2", bat=6)]}]},
            {"team": "Alpha", "super_over": True, "target": {"overs": 1, "runs": 7},
             "overs": [{"over": 0, "deliveries": [ball("A1", "B2", "A2", bat=1)]}]},
        ],
        outcome={"result": "tie", "eliminator": "Beta"})


def write_fixture_files(root, source="ipl_male"):
    """Write the test matches as Cricsheet files into root/source/. Returns the folder."""
    folder = os.path.join(root, source)
    os.makedirs(folder, exist_ok=True)
    files = {
        "1000001.json": json.dumps(match_a(), indent=2),
        "1000002.json": json.dumps(match_b(), indent=2),
        "1000003.json": json.dumps(match_c(), indent=2),
        "1000004.json": json.dumps(match(innings=[], outcome={"result": "draw"}, match_type="ODI")),
        "1000005.json": "{not valid json",
    }
    for name, text in files.items():
        with open(os.path.join(folder, name), "w") as f:
            f.write(text)
    return folder
