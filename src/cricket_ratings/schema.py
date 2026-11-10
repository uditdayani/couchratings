"""Spark schema for a Cricsheet JSON match file (https://cricsheet.org/format/json).

Only the fields the pipeline uses are listed; from_json ignores the rest.
Backticks guard names that are SQL keywords (group, by, over).
"""

MATCH_SCHEMA = """
meta STRUCT<data_version: STRING, created: STRING, revision: INT>,
info STRUCT<
  balls_per_over: INT,
  city: STRING,
  dates: ARRAY<STRING>,
  event: STRUCT<name: STRING, match_number: INT, `group`: STRING, stage: STRING>,
  gender: STRING,
  match_type: STRING,
  outcome: STRUCT<
    winner: STRING, result: STRING, method: STRING, eliminator: STRING, bowl_out: STRING,
    `by`: STRUCT<runs: INT, wickets: INT, innings: INT>>,
  overs: INT,
  player_of_match: ARRAY<STRING>,
  players: MAP<STRING, ARRAY<STRING>>,
  registry: STRUCT<people: MAP<STRING, STRING>>,
  season: STRING,
  team_type: STRING,
  teams: ARRAY<STRING>,
  toss: STRUCT<winner: STRING, decision: STRING>,
  venue: STRING>,
innings ARRAY<STRUCT<
  team: STRING,
  super_over: BOOLEAN,
  target: STRUCT<overs: DOUBLE, runs: INT>,
  penalty_runs: STRUCT<pre: INT, post: INT>,
  overs: ARRAY<STRUCT<
    `over`: INT,
    deliveries: ARRAY<STRUCT<
      batter: STRING,
      bowler: STRING,
      non_striker: STRING,
      runs: STRUCT<batter: INT, extras: INT, total: INT>,
      extras: STRUCT<wides: INT, noballs: INT, byes: INT, legbyes: INT, penalty: INT>,
      wickets: ARRAY<STRUCT<
        kind: STRING,
        player_out: STRING,
        fielders: ARRAY<STRUCT<name: STRING, substitute: BOOLEAN>>>>>>>>>>
"""

# Type of the raw wickets array, used to replace null with an empty array.
WICKETS_TYPE = (
    "array<struct<kind:string,player_out:string,"
    "fielders:array<struct<name:string,substitute:boolean>>>>"
)
FIELDERS_TYPE = "array<struct<name:string,substitute:boolean>>"

# Dismissal kinds that do not cost the batting side a wicket.
NOT_DISMISSALS = ("retired hurt", "retired not out")
