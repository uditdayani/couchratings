"""The SQL behind every dataset the dashboard shows. Plain Python (no Streamlit), so the
Databricks export notebook (09_export_parquet) runs exactly the same queries.

Each query takes a `:version` parameter (the model version); `balls` also takes `:match_id`.
"""

# Datasets every page needs, loaded up front; the others load the first time a page asks.
CORE = ("ratings", "matches", "form", "meta", "validation")
# Exported to Parquet as well: everything above plus these.
EXTRA = ("matchups", "balls", "photos")


def matches_query(c: str, venue_register: bool = True) -> str:
    """Matches with both innings' scores. With the venue register (silver.venue_names and
    silver.venues), each match also gets the register's venue id and unique name, and the
    event stage ("Final", ...). Without (tables built before V2.2), those columns are null."""
    venue_cols = ("vn.venue_id, v.unique_name AS venue_name, m.event_stage" if venue_register
                  else "CAST(NULL AS STRING) AS venue_id, CAST(NULL AS STRING) AS venue_name, "
                       "CAST(NULL AS STRING) AS event_stage")
    venue_joins = (f"""
            LEFT JOIN {c}.silver.venue_names vn ON vn.name = m.venue
            LEFT JOIN {c}.silver.venues v ON v.venue_id = vn.venue_id""" if venue_register else "")
    return f"""
            SELECT m.match_id, m.match_date, m.season, m.event_name, m.match_number, m.venue,
                   {venue_cols},
                   m.city, m.team_1, m.team_2, m.toss_winner, m.toss_decision, m.result,
                   m.winner, m.win_by_runs, m.win_by_wickets, m.method, m.super_over_winner,
                   m.player_of_match_ids,
                   i1.batting_team AS inn1_team, i1.total_runs AS inn1_runs,
                   i1.wickets AS inn1_wickets, i1.legal_balls AS inn1_balls,
                   i2.batting_team AS inn2_team, i2.total_runs AS inn2_runs,
                   i2.wickets AS inn2_wickets, i2.legal_balls AS inn2_balls
            FROM {c}.silver.matches m
            LEFT JOIN {c}.silver.innings i1 ON i1.match_id = m.match_id AND i1.innings_no = 1
            LEFT JOIN {c}.silver.innings i2 ON i2.match_id = m.match_id AND i2.innings_no = 2{venue_joins}
            WHERE m.match_id IN (SELECT DISTINCT match_id FROM {c}.gold.player_match_ratings
                                 WHERE version = :version)"""


def queries(c: str) -> dict:
    """SQL for each dataset. `c` is the catalog; :version is bound as a parameter."""
    return {
        "ratings": f"""
            SELECT r.*, p.name
            FROM {c}.gold.player_match_ratings r
            LEFT JOIN {c}.silver.players p ON p.player_id = r.player_id
            WHERE r.version = :version""",
        "matches": matches_query(c),
        "matchups": f"""
            SELECT competition, batter_id, bowler_id, matches, balls, runs, dismissals,
                   dots, fours, sixes, runs_vs_par
            FROM {c}.gold.matchups WHERE version = :version""",
        "photos": f"""
            SELECT player_id, image_url, page_url, artist, license
            FROM {c}.silver.player_photos""",
        "form": f"""
            SELECT scope, player_id, name, form_rating, matches_counted, last_match_date,
                   active, leaderboard_rank
            FROM {c}.gold.player_form WHERE version = :version""",
        "meta": f"""
            SELECT v.version, v.created_at, v.fitted_matches, v.first_match_date,
                   v.last_match_date AS fitted_to, v.params, v.notes,
                   (SELECT max(match_date) FROM {c}.silver.matches) AS latest_match,
                   (SELECT max(logged_at) FROM {c}.ops.ingest_log) AS last_ingest
            FROM {c}.gold.model_versions v WHERE v.version = :version""",
        "validation": f"""
            SELECT `check`, scope, value, target, passed, detail, checked_at
            FROM {c}.ops.validation_results
            WHERE version = :version AND run_id = (
              SELECT run_id FROM {c}.ops.validation_results WHERE version = :version
              ORDER BY checked_at DESC LIMIT 1)""",
    }




def balls_query(c: str, one_match: bool = True) -> str:
    """Ball-by-ball values for the momentum chart: one match (`:match_id`), or every match
    (for the Parquet export)."""
    where = "AND b.match_id = :match_id" if one_match else ""
    return f"""
            SELECT b.match_id, b.innings_no, b.seq, b.batting_team, b.batter_id, b.bowler_id,
                   b.is_legal, b.runs_batter, b.runs_total, b.dismissals,
                   try_element_at(b.wickets, 1).kind AS wicket_kind,
                   try_element_at(b.wickets, 1).player_out_id AS out_id,
                   b.b_before, round(b.expected_before, 2) AS expected_before,
                   round(b.expected_after, 2) AS expected_after, round(b.runs_added, 2) AS runs_added
            FROM {c}.gold.ball_values b
            WHERE b.version = :version {where}
            ORDER BY b.match_id, b.innings_no, b.seq"""
