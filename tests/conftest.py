"""Local Spark session for tests, set up to behave like Databricks serverless (ANSI SQL on).

The Delta Lake jars can't be downloaded in every environment, so the few Delta-only
writes in cricket_ratings.storage are swapped for plain-Spark equivalents with the
same behaviour. Everything else (parsing, table building, checks) runs unchanged.
"""
import os
import shutil
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from cricket_ratings import storage  # noqa: E402
from cricket_ratings.config import Tables, ensure_schemas  # noqa: E402


def _insert_new(spark, table, df, keys):
    existing = spark.table(table).select(*keys)
    df.join(existing, list(keys), "left_anti").write.mode("append").saveAsTable(table)


def _replace_matches(spark, table, df, match_ids):
    if not spark.catalog.tableExists(table):
        df.limit(0).write.saveAsTable(table)
    kept = spark.table(table).join(match_ids, "match_id", "left_anti")
    rewritten = kept.unionByName(df, allowMissingColumns=True).localCheckpoint()
    rewritten.write.mode("overwrite").saveAsTable(table)


def rewrite_table(spark, table, df):
    """Test helper: replace a table's contents with df (used to inject bad rows)."""
    df.localCheckpoint().write.mode("overwrite").saveAsTable(table)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="warehouse-")
    session = (
        SparkSession.builder.master("local[2]").appName("cricket-ratings-tests")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(autouse=True)
def plain_spark_storage(monkeypatch):
    monkeypatch.setattr(storage, "FORMAT", "parquet")
    monkeypatch.setattr(storage, "insert_new", _insert_new)
    monkeypatch.setattr(storage, "replace_matches", _replace_matches)


@pytest.fixture()
def tables(spark):
    """Fresh, empty schemas for each test."""
    t = Tables(catalog=None)
    for s in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    ensure_schemas(spark, t)
    return t


@pytest.fixture()
def json_root():
    root = tempfile.mkdtemp(prefix="cricsheet-")
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def run_id():
    return lambda: str(uuid.uuid4())
