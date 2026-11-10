"""Every table write goes through here, so the Delta-specific SQL lives in one place.

Local tests replace these functions with plain-Spark equivalents (see tests/conftest.py),
because the Delta jars are not available outside Databricks in the test environment.
"""
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession

FORMAT = "delta"


def create_table(spark: SparkSession, table: str, columns_ddl: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({columns_ddl}) USING {FORMAT}")


def append(df: DataFrame, table: str) -> None:
    df.write.format(FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(table)


def overwrite(df: DataFrame, table: str) -> None:
    df.write.format(FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def insert_new(spark: SparkSession, table: str, df: DataFrame, keys: Sequence[str]) -> None:
    """Insert only rows whose keys are not already in the table (idempotent)."""
    df.createOrReplaceTempView("_incoming")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"MERGE INTO {table} AS t USING _incoming AS s ON {on} WHEN NOT MATCHED THEN INSERT *")


def replace_matches(spark: SparkSession, table: str, df: DataFrame, match_ids: DataFrame) -> None:
    """Delete every row for these match_ids, then append the fresh rows."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format(FORMAT).saveAsTable(table)
    match_ids.createOrReplaceTempView("_batch_ids")
    spark.sql(f"MERGE INTO {table} AS t USING _batch_ids AS s ON t.match_id = s.match_id "
              "WHEN MATCHED THEN DELETE")
    append(df, table)
