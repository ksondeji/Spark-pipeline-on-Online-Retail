"""Outils pour vérifier le schéma et l'alignement des colonnes."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

from src.utils.logger import logger

_EXPECTED_SILVER_TYPES = {
    "InvoiceNo": StringType,
    "StockCode": StringType,
    "Description": StringType,
    "Quantity": IntegerType,
    "InvoiceDate": TimestampType,
    "UnitPrice": DoubleType,
    "CustomerID": StringType,
    "Country": StringType,
}


def inspect_dataframe_schema(df: DataFrame, label: str = "DataFrame") -> None:
    """
    Affiche schéma, types attendus (silver) et échantillon des colonnes clés.
    À appeler après clean_transactions pour valider df_silver.
    """
    logger.info("========== Schéma : %s ==========", label)
    df.printSchema()

    logger.info("--- Types par colonne ---")
    for field in df.schema.fields:
        logger.info("  %-15s %s", field.name, field.dataType.simpleString())

    if label.lower().endswith("silver") or "silver" in label.lower():
        logger.info("--- Contrôle types silver ---")
        for name, expected in _EXPECTED_SILVER_TYPES.items():
            if name not in df.columns:
                logger.warning("  Colonne manquante : %s", name)
                continue
            actual = type(df.schema[name].dataType)
            ok = issubclass(actual, expected) or actual == expected
            status = "OK" if ok else f"ATTENDU {expected.__name__}, REÇU {actual.__name__}"
            logger.info("  %-15s %s", name, status)

    logger.info("--- Échantillon (5 lignes) ---")
    cols = [
        c
        for c in (
            "InvoiceNo",
            "StockCode",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
        )
        if c in df.columns
    ]
    df.select(*cols).show(5, truncate=False)

    # Diagnostic léger sur raw uniquement (évite des count() lourds en silver)
    if "raw" in label.lower():
        if "Quantity" in df.columns:
            _log_suspicious_quantities(df, label)
        if "UnitPrice" in df.columns:
            _log_suspicious_unit_prices(df, label)


def _log_suspicious_quantities(df: DataFrame, label: str) -> None:
    """Détecte du texte (ex. METAL SIGN) dans Quantity avant cast."""
    if "Quantity" not in df.columns:
        return
    as_string = col("Quantity").cast("string")
    suspicious = df.filter(~as_string.rlike(r"^-?\d+$"))
    count = suspicious.limit(1000).count()
    if count > 0:
        logger.warning(
            "[%s] %s lignes avec Quantity non numérique (CSV probablement décalé)",
            label,
            count,
        )
        suspicious.select(
            "InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice"
        ).show(5, truncate=False)
    else:
        logger.info("[%s] Quantity : format numérique OK.", label)


def _log_suspicious_unit_prices(df: DataFrame, label: str) -> None:
    """Détecte des UnitPrice qui ressemblent à des dates (mauvais alignement CSV)."""
    as_string = col("UnitPrice").cast("string")
    suspicious = df.filter(as_string.rlike(r"^\d{1,2}/\d{1,2}/\d{4}"))
    count = suspicious.limit(1000).count()
    if count > 0:
        logger.warning(
            "[%s] %s lignes avec UnitPrice au format date (colonnes probablement décalées)",
            label,
            count,
        )
        suspicious.select("InvoiceNo", "Quantity", "InvoiceDate", "UnitPrice").show(
            5, truncate=False
        )
    else:
        logger.info("[%s] Aucun UnitPrice au format date détecté.", label)
