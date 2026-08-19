"""Comptage des rejets par règle de nettoyage (bronze = colonnes string)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.read_data import read_raw_csv
from src.ingestion.write_data import drop_corrupt_column
from src.transformations.cleaning import (
    _INVOICE_DATE_PATTERN,
    _QUANTITY_PATTERN,
    _UNIT_PRICE_PATTERN,
)
from src.utils.logger import logger


def _violation(condition: F.Column) -> F.Column:
    return F.when(condition, 1).otherwise(0)


def _default_raw_path(bronze_path: str) -> str | None:
    """Déduit le CSV raw à côté du dossier bronze (…/raw/bronze → …/Online_Retail.csv)."""
    base = bronze_path.rstrip("/")
    if base.endswith("/bronze"):
        return base.rsplit("/", 1)[0] + "/Online_Retail.csv"
    return None


def _load_bronze_as_strings(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Relit le Delta en forçant chaque colonne en STRING (évite parse timestamp)."""
    df = spark.read.format("delta").load(bronze_path)
    return df.select(
        [F.expr(f"try_cast(`{name}` AS STRING)").alias(name) for name in df.columns]
    )


def _load_for_breakdown(
    spark: SparkSession,
    bronze_path: str,
    raw_path: str | None,
) -> tuple[DataFrame, str]:
    """
    Préfère le CSV raw (schéma 100 % string) : la bronze Delta peut typer
    InvoiceDate en timestamp et faire échouer le parse de valeurs comme '2'.
    """
    if raw_path is None:
        raw_path = _default_raw_path(bronze_path)

    if raw_path:
        try:
            df = drop_corrupt_column(read_raw_csv(spark, raw_path))
            logger.info("Rejection breakdown : lecture CSV raw %s", raw_path)
            return df, raw_path
        except Exception as exc:
            logger.warning(
                "CSV raw illisible (%s), repli Delta bronze : %s", raw_path, exc
            )

    df = _load_bronze_as_strings(spark, bronze_path)
    logger.info("Rejection breakdown : lecture Delta bronze %s (colonnes en STRING)", bronze_path)
    return df, bronze_path


def get_rejection_breakdown(
    spark: SparkSession,
    bronze_path: str,
    *,
    raw_path: str | None = None,
) -> dict[str, int]:
    """
    Compte combien de lignes chaque règle de ``clean_transactions`` rejette.

    Chaque compteur est **indépendant** : les totaux peuvent se chevaucher.

    Par défaut lit ``Online_Retail.csv`` à côté de ``bronze`` (colonnes string).
    """
    df, _source = _load_for_breakdown(spark, bronze_path, raw_path)

    # Colonnes déjà string si CSV raw ; sinon projection try_cast ci-dessus
    qty_str = F.col("Quantity")
    price_str = F.col("UnitPrice")
    date_str = F.col("InvoiceDate")
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
