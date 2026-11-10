"""Silver data quality checks. Any failed `error` check stops the job before Gold.

Each check is a query returning one row per failing item (with a match_id),
so the result can show how many failed and a few example matches.
"""
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import SparkSession

from . import storage
from .config import Tables


@dataclass
class Check:
    name: str
    severity: str           # "error" stops the pipeline, "warning" is reported only
    failing_sql: str        # rows that break the rule; must include match_id
    eligible_sql: Optional[str] = None  # for rate-based checks: rows the rule applies to
    max_fail_rate: float = 0.0


def checks(t: Tables) -> List[Check]:
    m, i, d = t.silver_matches, t.silver_innings, t.silver_deliveries
    return [
        Check("unique_match_id", "error",
              f"SELECT match_id FROM {m} GROUP BY match_id HAVING count(*) > 1"),
        Check("unique_delivery_key", "error",
              f"SELECT match_id FROM {d} GROUP BY match_id, innings_no, seq HAVING count(*) > 1"),
        Check("only_t20_matches", "error",
              f"SELECT match_id FROM {m} WHERE match_type <> 'T20'"),
        Check("deliveries_have_a_match", "error",
              f"SELECT DISTINCT d.match_id FROM {d} d LEFT ANTI JOIN {m} m ON d.match_id = m.match_id"),
        Check("runs_total_adds_up", "error",
              f"SELECT match_id FROM {d} WHERE runs_total <> runs_batter + runs_extras"),
        Check("extras_breakdown_adds_up", "error",
              f"""SELECT match_id FROM {d} WHERE runs_extras <>
                  extras_wides + extras_noballs + extras_byes + extras_legbyes + extras_penalty"""),
        Check("player_ids_resolved", "error",
              f"""SELECT match_id FROM {d}
                  WHERE batter_id IS NULL OR bowler_id IS NULL OR non_striker_id IS NULL"""),
        # Second-innings target should equal first-innings runs + 1. Only clean cases
        # are checked (no D/L-style method, no penalty runs); a 1% tolerance keeps a
        # handful of source-data quirks from blocking the pipeline.
        Check("target_reconciles_with_first_innings", "error",
              failing_sql=f"""
                SELECT m.match_id FROM {m} m
                JOIN {i} i1 ON i1.match_id = m.match_id AND i1.innings_no = 1
                JOIN {i} i2 ON i2.match_id = m.match_id AND i2.innings_no = 2
                WHERE m.method IS NULL AND NOT i2.is_super_over AND i2.target_runs IS NOT NULL
                  AND i1.penalty_runs_pre + i1.penalty_runs_post + i2.penalty_runs_pre = 0
                  AND i2.target_runs <> i1.total_runs + 1""",
              eligible_sql=f"""
                SELECT m.match_id FROM {m} m
                JOIN {i} i1 ON i1.match_id = m.match_id AND i1.innings_no = 1
                JOIN {i} i2 ON i2.match_id = m.match_id AND i2.innings_no = 2
                WHERE m.method IS NULL AND NOT i2.is_super_over AND i2.target_runs IS NOT NULL
                  AND i1.penalty_runs_pre + i1.penalty_runs_post + i2.penalty_runs_pre = 0""",
              max_fail_rate=0.01),
        # A miscounted over can add a ball, so this one only warns.
        Check("legal_balls_within_overs_limit", "warning",
              f"""SELECT i.match_id FROM {i} i JOIN {m} m ON i.match_id = m.match_id
                  WHERE NOT i.is_super_over AND m.overs_limit IS NOT NULL
                    AND i.legal_balls > m.overs_limit * m.balls_per_over"""),
    ]


def run_checks(spark: SparkSession, t: Tables) -> List[dict]:
    results = []
    for c in checks(t):
        failing = spark.sql(c.failing_sql)
        n_fail = failing.count()
        examples = [r.match_id for r in failing.select("match_id").distinct().limit(5).collect()]
        if c.eligible_sql:
            n_eligible = spark.sql(c.eligible_sql).count()
            passed = n_fail <= c.max_fail_rate * n_eligible
            detail = f"{n_fail} of {n_eligible} eligible matches fail (limit {c.max_fail_rate:.0%})"
        else:
            passed = n_fail == 0
            detail = f"{n_fail} failing rows"
        if examples:
            detail += "; e.g. " + ", ".join(examples)
        results.append({"check_name": c.name, "severity": c.severity, "passed": passed,
                        "failing_count": n_fail, "detail": detail})
    return results


def save_and_enforce(spark: SparkSession, t: Tables, results: List[dict], run_id: str) -> None:
    """Append results to ops.dq_results, then raise if any error-level check failed."""
    from pyspark.sql import functions as F
    df = spark.createDataFrame(
        results, "check_name string, severity string, passed boolean, failing_count long, detail string")
    storage.append(df.withColumn("run_id", F.lit(run_id))
                     .withColumn("checked_at", F.current_timestamp()), t.dq_results)
    failed = [r["check_name"] for r in results if r["severity"] == "error" and not r["passed"]]
    if failed:
        raise RuntimeError(f"Silver data quality checks failed: {', '.join(failed)}")
