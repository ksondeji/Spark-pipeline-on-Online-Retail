"""Comparaisons de performance : partitionnement, temps, plans, historique Delta."""

from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession

REVENUE_BY_COUNTRY_SQL = """
SELECT
  Country,
  ROUND(SUM(OrderAmount), 2) AS total_revenue
FROM {table}
GROUP BY Country
ORDER BY total_revenue DESC
"""


def time_sql_query(
    spark: SparkSession,
    sql: str,
    *,
    action: Literal["collect", "show", "count"] = "collect",
) -> dict[str, Any]:
    """Mesure le temps d'exécution d'une requête SQL (action matérialisante)."""
    df = spark.sql(sql)
    start = time.perf_counter()
    if action == "collect":
        rows = len(df.collect())
    elif action == "count":
        rows = df.count()
    else:
        df.show(truncate=False)
        rows = None
    elapsed = time.perf_counter() - start
    return {
        "sql": sql.strip(),
        "action": action,
        "elapsed_seconds": elapsed,
        "row_count": rows,
    }


def benchmark_partitioned_vs_non_partitioned(
    spark: SparkSession,
    *,
    non_partitioned_table: str = "phase4",
    partitioned_table: str = "sales_per_country_continent",
    action: Literal["collect", "show", "count"] = "collect",
) -> dict[str, Any]:
    """
    Compare le CA par pays sur une table non partitionnée vs partitionnée
    (équivalent notebook : phase4 vs sales_per_country_continent).
    """
    sql_non_part = REVENUE_BY_COUNTRY_SQL.format(table=non_partitioned_table)
    sql_part = REVENUE_BY_COUNTRY_SQL.format(table=partitioned_table)

    non_part = time_sql_query(spark, sql_non_part, action=action)
    part = time_sql_query(spark, sql_part, action=action)

    non_part_time = non_part["elapsed_seconds"]
    part_time = part["elapsed_seconds"]
    speedup_pct = (
        (1 - part_time / non_part_time) * 100 if non_part_time > 0 else None
    )

    return {
        "non_partitioned": {
            "table": non_partitioned_table,
            **non_part,
        },
        "partitioned": {
            "table": partitioned_table,
            **part,
        },
        "speedup_seconds": non_part_time - part_time,
        "speedup_pct": speedup_pct,
        "faster": partitioned_table if part_time < non_part_time else non_partitioned_table,
    }


def get_query_plan(
    spark: SparkSession,
    sql: str,
    *,
    extended: bool = False,
) -> str:
    """Retourne le plan d'exécution (explain) d'une requête SQL."""
    df = spark.sql(sql)
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(extended)
    return buffer.getvalue()


def compare_query_plans(
    spark: SparkSession,
    sql_a: str,
    sql_b: str,
    *,
    extended: bool = False,
) -> dict[str, str]:
    """Compare les plans physiques/logiques de deux requêtes."""
    return {
        "plan_a": get_query_plan(spark, sql_a, extended=extended),
        "plan_b": get_query_plan(spark, sql_b, extended=extended),
    }


def get_table_history(spark: SparkSession, table: str) -> DataFrame:
    """Historique Delta (DESCRIBE HISTORY)."""
    return spark.sql(f"DESCRIBE HISTORY {table}")


def compare_revenue_by_country_versions(
    spark: SparkSession,
    *,
    table_v0: str,
    version_v0: int,
    table_v1: str,
    version_v1: int,
    label_v0: str = "v0",
    label_v1: str = "v1",
) -> DataFrame:
    """
    Écart de CA par pays entre deux versions Delta (time travel).
    Ex. phase1 VERSION 0 vs phase4 VERSION 1.
    """
    sql = f"""
    WITH revenues AS (
      SELECT '{label_v0}' AS version, Country,
             ROUND(SUM(OrderAmount), 2) AS total_revenue
      FROM {table_v0} VERSION AS OF {version_v0}
      GROUP BY Country

      UNION ALL

      SELECT '{label_v1}' AS version, Country,
             ROUND(SUM(OrderAmount), 2) AS total_revenue
      FROM {table_v1} VERSION AS OF {version_v1}
      GROUP BY Country
    )
    SELECT
      Country,
      MAX(CASE WHEN version = '{label_v0}' THEN total_revenue END) AS revenue_{label_v0},
      MAX(CASE WHEN version = '{label_v1}' THEN total_revenue END) AS revenue_{label_v1},
      ROUND(
        MAX(CASE WHEN version = '{label_v1}' THEN total_revenue END)
        - MAX(CASE WHEN version = '{label_v0}' THEN total_revenue END),
        2
      ) AS revenue_delta
    FROM revenues
    GROUP BY Country
    ORDER BY revenue_delta ASC
    """
    return spark.sql(sql)


def compare_total_revenue_versions(
    spark: SparkSession,
    versions: list[tuple[str, int, str]],
) -> dict[str, float]:
    """
    Compare le CA global pour plusieurs (table, version, label).

    Ex. versions=[("phase1", 0, "phase1_v0"), ("phase4", 1, "phase4_v1")]
    """
    totals: dict[str, float] = {}
    for table, version, label in versions:
        row = spark.sql(
            f"""
            SELECT ROUND(SUM(OrderAmount), 2) AS total_revenue
            FROM {table} VERSION AS OF {version}
            """
        ).collect()[0]
        totals[label] = float(row["total_revenue"])
    return totals


def summarize_revenue_deltas(
    spark: SparkSession,
    *,
    table_v0: str,
    version_v0: int,
    table_v1: str,
    version_v1: int,
    label_v0: str = "v0",
    label_v1: str = "v1",
) -> DataFrame:
    """Synthèse des deltas positifs / négatifs entre deux versions."""
    sql = f"""
    WITH revenues AS (
      SELECT '{label_v0}' AS version, Country,
             ROUND(SUM(OrderAmount), 2) AS total_revenue
      FROM {table_v0} VERSION AS OF {version_v0}
      GROUP BY Country

      UNION ALL

      SELECT '{label_v1}' AS version, Country,
             ROUND(SUM(OrderAmount), 2) AS total_revenue
      FROM {table_v1} VERSION AS OF {version_v1}
      GROUP BY Country
    ),
    deltas AS (
      SELECT
        Country,
        ROUND(
          MAX(CASE WHEN version = '{label_v1}' THEN total_revenue END)
          - MAX(CASE WHEN version = '{label_v0}' THEN total_revenue END),
          2
        ) AS revenue_delta
      FROM revenues
      GROUP BY Country
    )
    SELECT
      ROUND(SUM(CASE WHEN revenue_delta > 0 THEN revenue_delta ELSE 0 END), 2)
        AS total_positive_delta,
      ROUND(SUM(CASE WHEN revenue_delta < 0 THEN revenue_delta ELSE 0 END), 2)
        AS total_negative_delta
    FROM deltas
    """
    return spark.sql(sql)


def run_performance_report(
    spark: SparkSession,
    *,
    non_partitioned_table: str = "phase4",
    partitioned_table: str = "sales_per_country_continent",
    history_table: str = "phase4",
    table_v0: str = "phase1",
    version_v0: int = 0,
    table_v1: str = "phase4",
    version_v1: int = 1,
) -> dict[str, Any]:
    """
    Rapport agrégé : benchmark partitionnement, historique Delta,
    comparaison de versions (nécessite tables Delta existantes).
    """
    return {
        "partition_benchmark": benchmark_partitioned_vs_non_partitioned(
            spark,
            non_partitioned_table=non_partitioned_table,
            partitioned_table=partitioned_table,
        ),
        "history": get_table_history(spark, history_table),
        "revenue_by_country_delta": compare_revenue_by_country_versions(
            spark,
            table_v0=table_v0,
            version_v0=version_v0,
            table_v1=table_v1,
            version_v1=version_v1,
        ),
        "total_revenue_by_version": compare_total_revenue_versions(
            spark,
            [
                (table_v0, version_v0, f"{table_v0}_v{version_v0}"),
                (table_v1, version_v1, f"{table_v1}_v{version_v1}"),
            ],
        ),
        "revenue_delta_summary": summarize_revenue_deltas(
            spark,
            table_v0=table_v0,
            version_v0=version_v0,
            table_v1=table_v1,
            version_v1=version_v1,
        ),
    }
