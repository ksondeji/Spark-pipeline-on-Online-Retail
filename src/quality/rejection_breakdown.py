"""Comptage des rejets par règle de nettoyage (bronze = colonnes string)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformations.cleaning import (
    _INVOICE_DATE_PATTERN,
    _QUANTITY_PATTERN,
    _UNIT_PRICE_PATTERN,
)


def get_rejection_breakdown(spark: SparkSession, bronze_path: str) -> dict[str, int]:
    """
    Compte combien de lignes chaque règle de ``clean_transactions`` rejette.

    Chaque compteur est **indépendant** (calculé sur tout le bronze) : les totaux
    peuvent se chevaucher d'une ligne à l'autre.
    """
    df = spark.read.format("delta").load(bronze_path)
    total = df.count()

    qty_str = F.col("Quantity").cast("string")
    price_str = F.col("UnitPrice").cast("string")
    date_str = F.col("InvoiceDate").cast("string")
    qty_int = F.expr("try_cast(Quantity AS int)")
    price_dbl = F.expr("try_cast(UnitPrice AS double)")
    inv_date = F.to_timestamp(F.col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss")

    duplicate_groups = (
        df.groupBy("InvoiceNo", "StockCode")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    breakdown: dict[str, int] = {
        "Total bronze": total,
    }

    if "_corrupt_record" in df.columns:
        breakdown["_corrupt_record non null"] = df.filter(
            F.col("_corrupt_record").isNotNull()
        ).count()

    breakdown.update(
        {
            "CustomerID null": df.filter(F.col("CustomerID").isNull()).count(),
            "UnitPrice null": df.filter(F.col("UnitPrice").isNull()).count(),
            "Quantity null": df.filter(F.col("Quantity").isNull()).count(),
            "InvoiceNo null": df.filter(F.col("InvoiceNo").isNull()).count(),
            "Annulation (InvoiceNo c*)": df.filter(
                F.lower(F.col("InvoiceNo")).startswith("c")
            ).count(),
            "InvoiceNo 541431": df.filter(F.col("InvoiceNo") == "541431").count(),
            "StockCode longueur != 5": df.filter(F.length(F.col("StockCode")) != 5).count(),
            "Quantity format invalide": df.filter(~qty_str.rlike(_QUANTITY_PATTERN)).count(),
            "UnitPrice format invalide": df.filter(
                ~price_str.rlike(_UNIT_PRICE_PATTERN)
            ).count(),
            "UnitPrice contient / ou :": df.filter(price_str.rlike(r"[/:]")).count(),
            "InvoiceDate format invalide": df.filter(
                ~date_str.rlike(_INVOICE_DATE_PATTERN)
            ).count(),
            "Quantity non castable (int)": df.filter(qty_int.isNull()).count(),
            "UnitPrice non castable (double)": df.filter(price_dbl.isNull()).count(),
            "InvoiceDate non castable (timestamp)": df.filter(inv_date.isNull()).count(),
            "Quantity <= 0 (après cast)": df.filter(
                qty_int.isNotNull() & (qty_int <= 0)
            ).count(),
            "UnitPrice <= 0 (après cast)": df.filter(
                price_dbl.isNotNull() & (price_dbl <= 0)
            ).count(),
            "Doublons (InvoiceNo+StockCode)": duplicate_groups,
        }
    )

    return breakdown
