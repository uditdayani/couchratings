"""Silver layer: parse Bronze JSON into clean, typed tables.

Tables (grain):
  matches        one row per match
  innings        one row per innings, with totals computed from deliveries
  deliveries     one row per ball, including wides and no-balls
  match_players  one row per player per match
  players        one row per player (Cricsheet Register ID)

Each run processes only Bronze files the Silver stage has not seen (new files
or new versions). For those matches it deletes any old rows and inserts fresh
ones, so re-runs never duplicate data.
"""
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import storage
from .config import Tables
from .schema import FIELDERS_TYPE, MATCH_SCHEMA, NOT_DISMISSALS, WICKETS_TYPE

REG = "m.info.registry.people"


def _id(name_col) -> Column:
    """Look a player name up in this match's Cricsheet registry (null if missing)."""
    return F.try_element_at(F.col(REG), name_col)


# ---------------------------------------------------------------- selection

def current_bronze(spark: SparkSession, t: Tables) -> DataFrame:
    """Latest Bronze version of every file."""
    w = Window.partitionBy("file_name").orderBy(F.col("landed_at").desc(), F.col("file_hash"))
    return (spark.table(t.bronze_files)
            .withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))


def pending_files(spark: SparkSession, t: Tables) -> DataFrame:
    """Current Bronze files whose exact version Silver has not processed yet."""
    seen = (spark.table(t.ingest_log).filter(F.col("stage") == "silver")
            .select("file_name", "file_hash").distinct())
    return current_bronze(spark, t).join(seen, ["file_name", "file_hash"], "left_anti")


# ------------------------------------------------------------------ parsing

def parse(bronze: DataFrame) -> DataFrame:
    """Add the parsed match (`m`) and a quarantine reason (null when the file is usable)."""
    parsed = bronze.withColumn("m", F.from_json("raw_json", MATCH_SCHEMA))
    match_type = F.coalesce(F.col("m.info.match_type"), F.lit("missing"))
    reason = (
        F.when(F.col("m").isNull() | F.col("m.info").isNull(), F.lit("unparseable JSON"))
        .when(match_type != "T20", F.concat(F.lit("match_type is "), match_type))
        .when(F.col(REG).isNull(), F.lit("no player registry"))
        .when(F.col("m.info.teams").isNull() | (F.size("m.info.teams") != 2), F.lit("teams missing"))
    )
    return parsed.withColumn("quarantine_reason", reason)


# ----------------------------------------------------------- table builders

def build_matches(ok: DataFrame) -> DataFrame:
    i, o = "m.info", "m.info.outcome"
    pom = F.coalesce(F.col(f"{i}.player_of_match"), F.array().cast("array<string>"))
    return ok.select(
        "match_id",
        "source",
        "file_hash",
        F.to_date(F.try_element_at(F.col(f"{i}.dates"), F.lit(1))).alias("match_date"),
        F.col(f"{i}.season").alias("season"),
        F.col(f"{i}.event.name").alias("event_name"),
        F.col(f"{i}.event.match_number").alias("match_number"),
        F.col(f"{i}.gender").alias("gender"),
        F.col(f"{i}.match_type").alias("match_type"),
        F.col(f"{i}.team_type").alias("team_type"),
        F.col(f"{i}.overs").alias("overs_limit"),
        F.coalesce(F.col(f"{i}.balls_per_over"), F.lit(6)).alias("balls_per_over"),
        F.col(f"{i}.venue").alias("venue"),
        F.col(f"{i}.city").alias("city"),
        F.try_element_at(F.col(f"{i}.teams"), F.lit(1)).alias("team_1"),
        F.try_element_at(F.col(f"{i}.teams"), F.lit(2)).alias("team_2"),
        F.col(f"{i}.toss.winner").alias("toss_winner"),
        F.col(f"{i}.toss.decision").alias("toss_decision"),
        F.coalesce(F.col(f"{o}.result"),
                   F.when(F.col(f"{o}.winner").isNotNull(), F.lit("win"))).alias("result"),
        F.col(f"{o}.winner").alias("winner"),
        F.col(f"{o}.by.runs").alias("win_by_runs"),
        F.col(f"{o}.by.wickets").alias("win_by_wickets"),
        F.col(f"{o}.method").alias("method"),
        F.coalesce(F.col(f"{o}.eliminator"), F.col(f"{o}.bowl_out")).alias("super_over_winner"),
        F.transform(pom, lambda name: _id(name)).alias("player_of_match_ids"),
        F.col("m.meta.data_version").alias("data_version"),
        F.current_timestamp().alias("silver_updated_at"),
    )


def build_deliveries(ok: DataFrame) -> DataFrame:
    inn = ok.select("match_id", F.col(REG).alias("reg"),
                    F.posexplode("m.innings").alias("inn_idx", "inn"))
    ov = inn.select(
        "match_id", "reg",
        (F.col("inn_idx") + 1).alias("innings_no"),
        F.col("inn.team").alias("batting_team"),
        F.coalesce(F.col("inn.super_over"), F.lit(False)).alias("is_super_over"),
        F.explode("inn.overs").alias("ov"),
    )
    d = ov.select(
        "match_id", "reg", "innings_no", "batting_team", "is_super_over",
        F.col("ov.over").alias("over"),
        F.posexplode("ov.deliveries").alias("ball_idx", "d"),
    )

    reg = F.col("reg")
    is_legal = F.col("d.extras.wides").isNull() & F.col("d.extras.noballs").isNull()
    in_order = Window.partitionBy("match_id", "innings_no").orderBy("over", "ball_idx")
    so_far = in_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    raw_wickets = F.coalesce(F.col("d.wickets"), F.array().cast(WICKETS_TYPE))
    wickets = F.transform(raw_wickets, lambda w: F.struct(
        w["kind"].alias("kind"),
        F.try_element_at(reg, w["player_out"]).alias("player_out_id"),
        F.transform(F.coalesce(w["fielders"], F.array().cast(FIELDERS_TYPE)),
                    lambda f: F.try_element_at(reg, f["name"])).alias("fielder_ids"),
        F.exists(F.coalesce(w["fielders"], F.array().cast(FIELDERS_TYPE)),
                 lambda f: F.coalesce(f["substitute"], F.lit(False))).alias("substitute_fielder"),
    ))

    def extra(name: str) -> Column:
        return F.coalesce(F.col(f"d.extras.{name}"), F.lit(0)).alias(f"extras_{name}")

    return (
        d.withColumn("is_legal", is_legal)
        .withColumn("wickets", wickets)
        .select(
            "match_id", "innings_no",
            F.row_number().over(in_order).alias("seq"),
            "over",
            (F.col("ball_idx") + 1).alias("ball_in_over"),
            F.sum(F.col("is_legal").cast("int")).over(so_far).alias("legal_ball_no"),
            "is_legal", "batting_team", "is_super_over",
            F.try_element_at(reg, F.col("d.batter")).alias("batter_id"),
            F.try_element_at(reg, F.col("d.non_striker")).alias("non_striker_id"),
            F.try_element_at(reg, F.col("d.bowler")).alias("bowler_id"),
            F.col("d.runs.batter").alias("runs_batter"),
            F.col("d.runs.extras").alias("runs_extras"),
            F.col("d.runs.total").alias("runs_total"),
            extra("wides"), extra("noballs"), extra("byes"), extra("legbyes"), extra("penalty"),
            "wickets",
            F.size("wickets").alias("wicket_count"),
            F.size(F.filter("wickets", lambda w: ~w["kind"].isin(*NOT_DISMISSALS))).alias("dismissals"),
        )
    )


def build_innings(ok: DataFrame, deliveries: DataFrame) -> DataFrame:
    meta = ok.select("match_id", F.posexplode("m.innings").alias("inn_idx", "inn")).select(
        "match_id",
        (F.col("inn_idx") + 1).alias("innings_no"),
        F.col("inn.team").alias("batting_team"),
        F.coalesce(F.col("inn.super_over"), F.lit(False)).alias("is_super_over"),
        F.col("inn.target.runs").alias("target_runs"),
        F.col("inn.target.overs").alias("target_overs"),
        F.coalesce(F.col("inn.penalty_runs.pre"), F.lit(0)).alias("penalty_runs_pre"),
        F.coalesce(F.col("inn.penalty_runs.post"), F.lit(0)).alias("penalty_runs_post"),
    )
    totals = deliveries.groupBy("match_id", "innings_no").agg(
        F.sum("runs_total").alias("total_runs"),
        F.max("legal_ball_no").alias("legal_balls"),
        F.sum("dismissals").alias("wickets"),
    )
    return meta.join(totals, ["match_id", "innings_no"], "left")


def build_match_players(ok: DataFrame) -> DataFrame:
    teams = ok.select("match_id", F.col(REG).alias("reg"),
                      F.explode("m.info.players").alias("team", "names"))
    return teams.select(
        "match_id", "team",
        F.explode("names").alias("name"),
        "reg",
    ).select("match_id", "team", F.try_element_at(F.col("reg"), F.col("name")).alias("player_id"), "name")


def build_players(spark: SparkSession, t: Tables) -> DataFrame:
    """Rebuilt in full each run (small): the name each player used most recently."""
    mp = spark.table(t.silver_match_players).join(
        spark.table(t.silver_matches).select("match_id", "match_date"), "match_id")
    w = Window.partitionBy("player_id").orderBy(F.col("match_date").desc(), F.col("name"))
    latest = mp.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select("player_id", "name")
    stats = mp.groupBy("player_id").agg(
        F.countDistinct("match_id").alias("matches"),
        F.min("match_date").alias("first_match_date"),
        F.max("match_date").alias("last_match_date"),
    )
    return latest.join(stats, "player_id").filter(F.col("player_id").isNotNull())


# ------------------------------------------------------------------ writing

def build_silver(spark: SparkSession, t: Tables, run_id: str) -> dict:
    """Process pending Bronze files into Silver. Returns counts for the notebook to show."""
    # Fix the batch first: everything below must see the same set of files.
    batch = pending_files(spark, t).select("file_name", "file_hash")
    batch_rows = batch.collect()
    if not batch_rows:
        return {"parsed": 0, "quarantined": 0}
    batch = spark.createDataFrame(batch_rows, "file_name string, file_hash string")
    parsed = parse(spark.table(t.bronze_files).join(batch, ["file_name", "file_hash"]))
    ok = parsed.filter(F.col("quarantine_reason").isNull())

    ids = ok.select("match_id").distinct()
    n_ok = ids.count()
    if n_ok:
        storage.replace_matches(spark, t.silver_deliveries, build_deliveries(ok), ids)
        batch_deliveries = spark.table(t.silver_deliveries).join(ids, "match_id")
        storage.replace_matches(spark, t.silver_innings, build_innings(ok, batch_deliveries), ids)
        storage.replace_matches(spark, t.silver_match_players, build_match_players(ok), ids)
        storage.replace_matches(spark, t.silver_matches, build_matches(ok), ids)
        storage.overwrite(build_players(spark, t), t.silver_players)

    # Logged last: a run that dies midway simply reprocesses these files next time.
    log = parsed.select(
        F.lit(run_id).alias("run_id"), F.lit("silver").alias("stage"),
        "file_name", "match_id", "file_hash",
        F.when(F.col("quarantine_reason").isNull(), F.lit("parsed"))
         .otherwise(F.lit("quarantined")).alias("status"),
        F.col("quarantine_reason").alias("detail"),
        F.current_timestamp().alias("logged_at"),
    )
    storage.append(log, t.ingest_log)
    return {"parsed": n_ok, "quarantined": len(batch_rows) - n_ok}
