"""Bronze layer: land raw Cricsheet JSON files unchanged, once per version.

Bronze is append-only. A file is identified by (file_name, file_hash), so a
re-run adds nothing, and a file Cricsheet later revises lands as a new version.
"""
import os
import shutil
import zipfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import storage
from .config import Tables


def extract_zip(zip_path: str, out_dir: str) -> int:
    """Extract the match JSON files from a Cricsheet zip into out_dir. Returns the file count."""
    os.makedirs(out_dir, exist_ok=True)
    count = 0
    with zipfile.ZipFile(zip_path) as z:
        for member in z.namelist():
            if not member.endswith(".json"):
                continue  # skips the README.txt inside each zip
            name = os.path.basename(member)  # never trust paths inside an archive
            with z.open(member) as src, open(os.path.join(out_dir, name), "wb") as dst:
                shutil.copyfileobj(src, dst)
            count += 1
    return count


def ensure_tables(spark: SparkSession, t: Tables) -> None:
    storage.create_table(spark, t.bronze_files,
                         "file_name STRING, match_id STRING, source STRING, file_hash STRING, "
                         "raw_json STRING, run_id STRING, landed_at TIMESTAMP")
    storage.create_table(spark, t.ingest_log,
                         "run_id STRING, stage STRING, file_name STRING, match_id STRING, "
                         "file_hash STRING, status STRING, detail STRING, logged_at TIMESTAMP")


def read_raw_files(spark: SparkSession, json_glob: str, run_id: str) -> DataFrame:
    """One row per JSON file. `source` is the name of the folder the file sits in."""
    # wholetext must be an argument: .text() has its own default that overrides .option().
    raw = spark.read.text(json_glob, wholetext=True)
    return (
        raw.select(
            F.col("_metadata.file_name").alias("file_name"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("value").alias("raw_json"),
        )
        .select(
            "file_name",
            F.regexp_replace("file_name", r"\.json$", "").alias("match_id"),
            F.regexp_extract("file_path", r"/([^/]+)/[^/]+$", 1).alias("source"),
            F.sha2("raw_json", 256).alias("file_hash"),
            "raw_json",
            F.lit(run_id).alias("run_id"),
            F.current_timestamp().alias("landed_at"),
        )
        .dropDuplicates(["file_name", "file_hash"])
    )


def land(spark: SparkSession, t: Tables, incoming: DataFrame, run_id: str) -> int:
    """Insert files not already in Bronze, log them, and return how many were new."""
    storage.insert_new(spark, t.bronze_files, incoming, keys=["file_name", "file_hash"])
    new = spark.table(t.bronze_files).filter(F.col("run_id") == run_id)
    storage.append(new.select(
        "run_id", F.lit("bronze").alias("stage"), "file_name", "match_id", "file_hash",
        F.lit("landed").alias("status"), F.col("source").alias("detail"),
        F.current_timestamp().alias("logged_at")), t.ingest_log)
    return new.count()
