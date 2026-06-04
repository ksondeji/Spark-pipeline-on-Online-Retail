from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, length, lower, try_cast


class DataQualityError(Exception):
    """Levée lorsqu'au moins une contrainte métier n'est pas respectée."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        failed = [c["name"] for c in report["constraints"] if not c["passed"]]
        super().__init__(
            f"{len(failed)} contrainte(s) en échec : {', '.join(failed)}"
        )


# Contrôles sur données BRUTES (string) — optionnel, avant nettoyage
RAW_CONSTRAINTS: list[tuple[str, Column]] = [
    ("customer_id_not_null", col("CustomerID").isNull()),
    ("invoice_no_not_null", col("InvoiceNo").isNull()),
    ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
    ("invoice_not_541431", col("InvoiceNo") == "541431"),
    ("stock_code_length_5", length(col("StockCode")) != 5),
]

# Silver relu depuis Delta : uniquement colonnes string (évite CAST implicite Photon)
SILVER_CONSTRAINTS: list[tuple[str, Column]] = [
    ("customer_id_not_null", col("CustomerID").isNull()),
    ("invoice_no_not_null", col("InvoiceNo").isNull()),
    ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
    ("invoice_not_541431", col("InvoiceNo") == "541431"),
    ("stock_code_length_5", length(col("StockCode")) != 5),
]

ENRICHED_CONSTRAINTS: list[tuple[str, Column]] = [
    ("item_code_length_5", length(col("ItemCode")) != 5),
    (
        "order_amount_positive",
        try_cast(col("OrderAmount"), "double").isNull()
        | (try_cast(col("OrderAmount"), "double") <= 0),
    ),
]

CLEANING_CONSTRAINTS = SILVER_CONSTRAINTS


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
        constraints_def = RAW_CONSTRAINTS
    elif scope in ("cleaning", "silver"):
        constraints_def = SILVER_CONSTRAINTS
    elif scope == "enriched":
        constraints_def = ENRICHED_CONSTRAINTS
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
