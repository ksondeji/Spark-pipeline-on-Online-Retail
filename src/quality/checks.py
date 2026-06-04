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


# Expressions Column (évite les casts SQL implicites sur Databricks Connect)
CLEANING_CONSTRAINTS: list[tuple[str, Column]] = [
    ("customer_id_not_null", col("CustomerID").isNull()),
    ("unit_price_not_null", col("UnitPrice").isNull()),
    ("quantity_not_null", col("Quantity").isNull()),
    ("invoice_no_not_null", col("InvoiceNo").isNull()),
    ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
    ("invoice_not_541431", col("InvoiceNo") == "541431"),
    ("stock_code_length_5", length(col("StockCode")) != 5),
    ("quantity_positive", col("Quantity") <= 0),
    ("unit_price_positive", col("UnitPrice") <= 0),
    ("invoice_date_not_null", col("InvoiceDate").isNull()),
]

ENRICHED_CONSTRAINTS: list[tuple[str, Column]] = [
    ("order_amount_positive", col("OrderAmount") <= 0),
    ("item_code_length_5", length(col("ItemCode")) != 5),
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
    scope: str = "cleaning",
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """
    Vérifie les contraintes alignées sur clean_transactions (scope='cleaning')
    ou sur les tables enrichies (scope='enriched').
    """
    if scope == "cleaning":
        constraints_def = CLEANING_CONSTRAINTS
    elif scope == "enriched":
        constraints_def = ENRICHED_CONSTRAINTS
    else:
        raise ValueError("scope doit être 'cleaning' ou 'enriched'")

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
