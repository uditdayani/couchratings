"""Table names in one place, so notebooks and tests never hard-code them."""
from dataclasses import dataclass
from typing import Optional

SCHEMAS = ("bronze", "silver", "gold", "ops")


@dataclass(frozen=True)
class Tables:
    """`catalog="workspace"` on Databricks Free Edition; `catalog=None` for local tests."""

    catalog: Optional[str] = "workspace"

    def schema(self, name: str) -> str:
        return f"{self.catalog}.{name}" if self.catalog else name

    def _t(self, schema: str, name: str) -> str:
        return f"{self.schema(schema)}.{name}"

    @property
    def bronze_files(self) -> str:
        return self._t("bronze", "match_files")

    @property
    def silver_matches(self) -> str:
        return self._t("silver", "matches")

    @property
    def silver_innings(self) -> str:
        return self._t("silver", "innings")

    @property
    def silver_deliveries(self) -> str:
        return self._t("silver", "deliveries")

    @property
    def silver_match_players(self) -> str:
        return self._t("silver", "match_players")

    @property
    def silver_players(self) -> str:
        return self._t("silver", "players")

    @property
    def ingest_log(self) -> str:
        return self._t("ops", "ingest_log")

    @property
    def dq_results(self) -> str:
        return self._t("ops", "dq_results")


def ensure_schemas(spark, t: Tables) -> None:
    for s in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t.schema(s)}")
