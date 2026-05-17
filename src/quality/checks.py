from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame


class DataQualityError(Exception):
    """Levée lorsqu'au moins une contrainte métier n'est pas respectée."""

    def __init__(self, report: dict[str, Any]):
        self.report = report
        failed = [c["name"] for c in report["constraints"] if not c["passed"]]
        super().__init__(
            f"{len(failed)} contrainte(s) en échec : {', '.join(failed)}"
        )


# Expressions SQL : lignes qui violent la contrainte (alignées sur clean_transactions)
CLEANING_CONSTRAINTS: list[tuple[str, str]] = [
    ("customer_id_not_null", "CustomerID IS NULL"),
    ("unit_price_not_null", "UnitPrice IS NULL"),
    ("quantity_not_null", "Quantity IS NULL"),
    ("invoice_no_not_null", "InvoiceNo IS NULL"),
    ("invoice_not_cancelled", "lower(InvoiceNo) LIKE 'c%'"),
    ("invoice_not_541431", "InvoiceNo = '541431'"),
    ("stock_code_length_5", "length(StockCode) <> 5"),
]

ENRICHED_CONSTRAINTS: list[tuple[str, str]] = [
    ("order_amount_positive", "OrderAmount <= 0"),
    ("item_code_length_5", "length(ItemCode) <> 5"),
]


def _evaluate_constraints(
    df: DataFrame, constraints: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    results = []
    for name, violation_sql in constraints:
        violations = df.filter(violation_sql).count()
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

    Retourne un rapport structuré. Si raise_on_failure=True et qu'une contrainte
    échoue, lève DataQualityError avec le rapport en attribut .report.
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
