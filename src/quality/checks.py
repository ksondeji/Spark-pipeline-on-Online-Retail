from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, length, lower


class DataQualityError(Exception):
    """Levée lorsqu'au moins une contrainte métier n'est pas respectée."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        failed = [c["name"] for c in report["constraints"] if not c["passed"]]
        super().__init__(
            f"{len(failed)} contrainte(s) en échec : {', '.join(failed)}"
        )


def _raw_constraints() -> list[tuple[str, Column]]:
    """Construites à l'appel (après SparkSession), pas à l'import."""
    return [
        ("customer_id_not_null", col("CustomerID").isNull()),
        ("invoice_no_not_null", col("InvoiceNo").isNull()),
        ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
        ("invoice_not_541431", col("InvoiceNo") == "541431"),
        ("stock_code_length_5", length(col("StockCode")) != 5),
    ]


def _silver_constraints() -> list[tuple[str, Column]]:
    return [
        ("customer_id_not_null", col("CustomerID").isNull()),
        ("invoice_no_not_null", col("InvoiceNo").isNull()),
        ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
        ("invoice_not_541431", col("InvoiceNo") == "541431"),
        ("stock_code_length_5", length(col("StockCode")) != 5),
    ]


def _enriched_constraints() -> list[tuple[str, Column]]:
    return [
        ("item_code_length_5", length(col("ItemCode")) != 5),
        (
            "order_amount_positive",
            col("OrderAmount").isNull() | (col("OrderAmount") <= 0),
        ),
    ]


def _evaluate_constraints(
    df: DataFrame, constraints: list[tuple[str, Column]]
) -> list[dict[str, Any]]:
    results = []
    for name, violation_expr in constraints:
        violations = df.filter(violation_expr).count()
        results.append(
            {
                "name": name,
                "violations": violations,
                "passed": violations == 0,
            }
        )
    return results


def run_checks(
    df: DataFrame,
    *,
    scope: str = "silver",
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """
    Vérifie les contraintes métier.

    À utiliser sur un DataFrame relu depuis Delta (types stables).
    Les contrôles numériques sont faits dans clean_transactions, pas ici sur silver.
    """
    if scope == "raw":
        constraints_def = _raw_constraints()
    elif scope in ("cleaning", "silver"):
        constraints_def = _silver_constraints()
    elif scope == "enriched":
        constraints_def = _enriched_constraints()
    else:
        raise ValueError("scope doit être 'raw', 'silver', 'cleaning' ou 'enriched'")

    constraint_results = _evaluate_constraints(df, constraints_def)
    report: dict[str, Any] = {
        "scope": scope,
        "row_count": df.count(),
        "constraints": constraint_results,
        "passed": all(c["passed"] for c in constraint_results),
    }

    if not report["passed"] and raise_on_failure:
        raise DataQualityError(report)

    return report
