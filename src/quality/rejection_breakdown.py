"""Comptage des rejets par règle de nettoyage (bronze = colonnes string)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformations.cleaning import (
    _INVOICE_DATE_PATTERN,
    _QUANTITY_PATTERN,
    _UNIT_PRICE_PATTERN,
)


def _violation(condition: F.Column) -> F.Column:
    return F.when(condition, 1).otherwise(0)


def get_rejection_breakdown(spark: SparkSession, bronze_path: str) -> dict[str, int]:
    """
    Compte combien de lignes chaque règle de ``clean_transactions`` rejette.

    Chaque compteur est **indépendant** (calculé sur tout le bronze) : les totaux
    peuvent se chevaucher. Aucun ``to_timestamp`` / ``try_to_timestamp`` ici :
    sur bronze string, le parse strict Databricks lève encore SQLSTATE 22007.
    """
    df = spark.read.format("delta").load(bronze_path)

    qty_str = F.col("Quantity").cast("string")
    price_str = F.col("UnitPrice").cast("string")
    date_str = F.col("InvoiceDate").cast("string")
    qty_int = F.expr("try_cast(Quantity AS int)")
    price_dbl = F.expr("try_cast(UnitPrice AS double)")

    agg_exprs = [
        F.count(F.lit(1)).alias("Total bronze"),
        F.sum(_violation(F.col("CustomerID").isNull())).alias("CustomerID null"),
        F.sum(_violation(F.col("UnitPrice").isNull())).alias("UnitPrice null"),
        F.sum(_violation(F.col("Quantity").isNull())).alias("Quantity null"),
        F.sum(_violation(F.col("InvoiceNo").isNull())).alias("InvoiceNo null"),
        F.sum(_violation(F.lower(F.col("InvoiceNo")).startswith("c"))).alias(
            "Annulation (InvoiceNo c*)"
        ),
        F.sum(_violation(F.col("InvoiceNo") == "541431")).alias("InvoiceNo 541431"),
        F.sum(_violation(F.length(F.col("StockCode")) != 5)).alias(
            "StockCode longueur != 5"
        ),
        F.sum(_violation(~qty_str.rlike(_QUANTITY_PATTERN))).alias(
            "Quantity format invalide"
        ),
        F.sum(_violation(~price_str.rlike(_UNIT_PRICE_PATTERN))).alias(
            "UnitPrice format invalide"
        ),
        F.sum(_violation(price_str.rlike(r"[/:]"))).alias("UnitPrice contient / ou :"),
        F.sum(_violation(~date_str.rlike(_INVOICE_DATE_PATTERN))).alias(
            "InvoiceDate format invalide"
        ),
        F.sum(_violation(qty_int.isNull())).alias("Quantity non castable (int)"),
        F.sum(_violation(price_dbl.isNull())).alias("UnitPrice non castable (double)"),
        F.sum(_violation(qty_int.isNotNull() & (qty_int <= 0))).alias(
            "Quantity <= 0 (après cast)"
        ),
        F.sum(_violation(price_dbl.isNotNull() & (price_dbl <= 0))).alias(
            "UnitPrice <= 0 (après cast)"
        ),
    ]

    if "_corrupt_record" in df.columns:
        agg_exprs.insert(
            1,
            F.sum(_violation(F.col("_corrupt_record").isNotNull())).alias(
                "_corrupt_record non null"
            ),
        )

    row = df.agg(*agg_exprs).first()
    breakdown = {field: int(row[field]) for field in row.asDict()}

    breakdown["Doublons (InvoiceNo+StockCode)"] = (
        df.groupBy("InvoiceNo", "StockCode")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return breakdown
