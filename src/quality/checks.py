from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, length, lit, lower, try_cast


class DataQualityError(Exception):
    """Levée lorsqu'au moins une contrainte métier n'est pas respectée."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        failed = [c["name"] for c in report["constraints"] if not c["passed"]]
        super().__init__(
            f"{len(failed)} contrainte(s) en échec : {', '.join(failed)}"
        )


def _non_positive(column: str, spark_type: str) -> Column:
    """Violation sans cast strict (compatible Databricks ANSI / Connect)."""
    typed = try_cast(col(column), spark_type)
    zero = lit(0) if spark_type == "int" else lit(0.0)
    return typed.isNull() | (typed <= zero)


# Contrôles sur données BRUTES (string) — avant clean_transactions
RAW_CONSTRAINTS: list[tuple[str, Column]] = [
    ("customer_id_not_null", col("CustomerID").isNull()),
    ("unit_price_not_null", col("UnitPrice").isNull()),
    ("quantity_not_null", col("Quantity").isNull()),
    ("invoice_no_not_null", col("InvoiceNo").isNull()),
    ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
    ("invoice_not_541431", col("InvoiceNo") == "541431"),
    ("stock_code_length_5", length(col("StockCode")) != 5),
]

# Contrôles sur SILVER (après clean) — pas de comparaison numérique directe sur col()
SILVER_CONSTRAINTS: list[tuple[str, Column]] = [
    ("customer_id_not_null", col("CustomerID").isNull()),
    ("unit_price_not_null", col("UnitPrice").isNull()),
    ("quantity_not_null", col("Quantity").isNull()),
    ("invoice_no_not_null", col("InvoiceNo").isNull()),
    ("invoice_date_not_null", col("InvoiceDate").isNull()),
    ("invoice_not_cancelled", lower(col("InvoiceNo")).startswith("c")),
    ("invoice_not_541431", col("InvoiceNo") == "541431"),
    ("stock_code_length_5", length(col("StockCode")) != 5),
    ("quantity_positive", _non_positive("Quantity", "int")),
    ("unit_price_positive", _non_positive("UnitPrice", "double")),
]

ENRICHED_CONSTRAINTS: list[tuple[str, Column]] = [
    ("order_amount_positive", _non_positive("OrderAmount", "double")),
    ("item_code_length_5", length(col("ItemCode")) != 5),
]

# Rétrocompatibilité
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

    - ``raw`` : données CSV brutes (string)
    - ``silver`` ou ``cleaning`` : après clean_transactions (types castés)
    - ``enriched`` : couche gold
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
