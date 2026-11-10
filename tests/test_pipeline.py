"""Bronze landing, Silver building and the quality checks, end to end on test matches."""
import json
import os
import zipfile

import pytest
from pyspark.sql import functions as F

from conftest import rewrite_table
from cricket_ratings import bronze, quality, silver
from factories import match_a, write_fixture_files


def land_and_build(spark, t, json_root, new_run_id):
    bronze.ensure_tables(spark, t)
    rid = new_run_id()
    incoming = bronze.read_raw_files(spark, f"{json_root}/*/*.json", rid)
    landed = bronze.land(spark, t, incoming, rid)
    counts = silver.build_silver(spark, t, new_run_id())
    return landed, counts


@pytest.fixture()
def built(spark, tables, json_root, run_id):
    write_fixture_files(json_root)
    landed, counts = land_and_build(spark, tables, json_root, run_id)
    return tables, landed, counts


def rows(spark, table, where="true", order="match_id"):
    cols = [c.strip() for c in order.split(",")]
    return [r.asDict() for r in spark.table(table).where(where).orderBy(*cols).collect()]


# ------------------------------------------------------------------- bronze

def test_extract_zip_skips_readme_and_paths(tmp_path):
    zpath = tmp_path / "ipl_male_json.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("README.txt", "not a match")
        z.writestr("nested/1000001.json", "{}")
    out = tmp_path / "out"
    assert bronze.extract_zip(str(zpath), str(out)) == 1
    assert os.listdir(out) == ["1000001.json"]


def test_bronze_lands_every_file_once(spark, built):
    t, landed, _ = built
    assert landed == 5
    files = rows(spark, t.bronze_files, order="file_name")
    assert [f["match_id"] for f in files] == ["1000001", "1000002", "1000003", "1000004", "1000005"]
    assert {f["source"] for f in files} == {"ipl_male"}


def test_rerun_is_idempotent(spark, built, json_root, run_id):
    t, _, _ = built
    before = {tbl: spark.table(tbl).count() for tbl in
              (t.bronze_files, t.silver_matches, t.silver_deliveries, t.silver_innings, t.silver_match_players)}
    landed, counts = land_and_build(spark, t, json_root, run_id)
    assert landed == 0
    assert counts == {"parsed": 0, "quarantined": 0}
    after = {tbl: spark.table(tbl).count() for tbl in before}
    assert after == before


def test_revised_file_replaces_old_rows(spark, built, json_root, run_id):
    t, _, _ = built
    revised = match_a()
    revised["innings"][0]["overs"].pop()  # Cricsheet corrects the file: one over fewer
    revised["innings"][1]["target"]["runs"] = 12
    with open(os.path.join(json_root, "ipl_male", "1000001.json"), "w") as f:
        json.dump(revised, f)

    landed, counts = land_and_build(spark, t, json_root, run_id)
    assert landed == 1                                   # new version lands next to the old one
    assert spark.table(t.bronze_files).where("match_id = '1000001'").count() == 2
    assert counts["parsed"] == 1
    inn1 = rows(spark, t.silver_deliveries, "match_id = '1000001' AND innings_no = 1", order="seq")
    assert len(inn1) == 8                                # old over 1 is gone, not duplicated
    assert spark.table(t.silver_matches).where("match_id = '1000001'").count() == 1


# ------------------------------------------------------------------- silver

def test_bad_files_are_quarantined_with_reason(spark, built):
    t, _, counts = built
    assert counts == {"parsed": 3, "quarantined": 2}
    q = rows(spark, t.ingest_log, "stage = 'silver' AND status = 'quarantined'")
    assert {r["match_id"]: r["detail"] for r in q} == {
        "1000004": "match_type is ODI", "1000005": "unparseable JSON"}


def test_matches_outcomes(spark, built):
    t, _, _ = built
    m = {r["match_id"]: r for r in rows(spark, t.silver_matches)}
    assert set(m) == {"1000001", "1000002", "1000003"}
    a = m["1000001"]
    assert (a["result"], a["winner"], a["win_by_runs"]) == ("win", "Alpha", 11)
    assert a["player_of_match_ids"] == ["a0000001"]
    assert str(a["match_date"]) == "2025-05-01" and (a["team_1"], a["team_2"]) == ("Alpha", "Beta")
    assert m["1000002"]["result"] == "no result" and m["1000002"]["winner"] is None
    assert m["1000002"]["season"] == "2025"           # numeric season read as text
    assert (m["1000003"]["result"], m["1000003"]["super_over_winner"]) == ("tie", "Beta")


def test_deliveries_count_legal_balls(spark, built):
    t, _, _ = built
    inn1 = rows(spark, t.silver_deliveries, "match_id = '1000001' AND innings_no = 1", order="seq")
    assert [d["seq"] for d in inn1] == list(range(1, 11))
    assert [d["legal_ball_no"] for d in inn1] == [1, 1, 2, 3, 3, 4, 5, 6, 7, 8]
    assert [d["is_legal"] for d in inn1][:5] == [True, False, True, True, False]
    noball = inn1[4]
    assert (noball["runs_batter"], noball["extras_noballs"], noball["runs_total"]) == (2, 1, 3)
    assert inn1[0]["batter_id"] == "a0000001" and inn1[0]["bowler_id"] == "b0000001"


def test_wickets_and_fielders(spark, built):
    t, _, _ = built
    w = rows(spark, t.silver_deliveries, "match_id = '1000001' AND wicket_count > 0",
             order="innings_no, seq")
    caught, run_out, bowled = (r["wickets"][0] for r in w)
    assert (caught["kind"], caught["player_out_id"], caught["fielder_ids"]) == ("caught", "a0000002", ["b0000002"])
    assert caught["substitute_fielder"] is False
    assert run_out["fielder_ids"] == ["b0000003"] and run_out["substitute_fielder"] is True
    assert bowled["fielder_ids"] == [] and bowled["player_out_id"] == "b0000002"


def test_innings_totals_and_super_overs(spark, built):
    t, _, _ = built
    inn = {(r["match_id"], r["innings_no"]): r for r in rows(spark, t.silver_innings)}
    a1 = inn[("1000001", 1)]
    assert (a1["total_runs"], a1["legal_balls"], a1["wickets"]) == (17, 8, 2)
    assert inn[("1000001", 2)]["target_runs"] == 18
    assert [inn[("1000003", n)]["is_super_over"] for n in (1, 2, 3, 4)] == [False, False, True, True]


def test_players_exclude_officials(spark, built):
    t, _, _ = built
    players = rows(spark, t.silver_players, order="player_id")
    assert [p["player_id"] for p in players] == [
        "a0000001", "a0000002", "a0000003", "b0000001", "b0000002", "b0000003"]
    assert players[0]["matches"] == 3


# ------------------------------------------------------------------ quality

def test_checks_pass_on_clean_data(spark, built):
    t, _, _ = built
    results = quality.run_checks(spark, t)
    assert all(r["passed"] for r in results), results
    quality.save_and_enforce(spark, t, results, "run-1")      # does not raise
    assert spark.table(t.dq_results).count() == len(results)


def test_checks_catch_broken_rows(spark, built):
    t, _, _ = built
    d = spark.table(t.silver_deliveries)
    rewrite_table(spark, t.silver_deliveries, d.withColumn(
        "runs_total", F.when((F.col("match_id") == "1000001") & (F.col("seq") == 1), F.lit(99))
                       .otherwise(F.col("runs_total"))))
    i = spark.table(t.silver_innings)
    rewrite_table(spark, t.silver_innings, i.withColumn(
        "target_runs", F.when((F.col("match_id") == "1000003") & (F.col("innings_no") == 2), F.lit(30))
                        .otherwise(F.col("target_runs"))))
    results = {r["check_name"]: r for r in quality.run_checks(spark, t)}
    assert not results["runs_total_adds_up"]["passed"]
    assert "1000001" in results["runs_total_adds_up"]["detail"]
    assert not results["target_reconciles_with_first_innings"]["passed"]
    with pytest.raises(RuntimeError, match="runs_total_adds_up"):
        quality.save_and_enforce(spark, t, list(results.values()), "run-2")
