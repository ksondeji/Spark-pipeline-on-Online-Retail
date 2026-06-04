from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# Toutes les colonnes métier en string ; cast dans cleaning.py
# _corrupt_record : lignes CSV non parsables (mode PERMISSIVE)
RAW_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

# Alias conservé
ONLINE_RETAIL_SCHEMA = RAW_SCHEMA


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Lit le CSV brut en mode PERMISSIVE : les lignes mal formées
    sont isolées dans _corrupt_record au lieu de faire échouer la lecture.
    """
    return (
        spark.read.option("header", True)
        .option("encoding", "ISO-8859-1")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("quote", '"')
        .option("escape", '"')
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def load_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Alias conservé pour compatibilité."""
    return read_raw_csv(spark, path)
