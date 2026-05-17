from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

ONLINE_RETAIL_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
    ]
)


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Lit le CSV brut Online Retail avec un schéma explicite."""
    return (
        spark.read.option("header", True)
        .schema(ONLINE_RETAIL_SCHEMA)
        .csv(path)
    )


def load_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Alias conservé pour compatibilité."""
    return read_raw_csv(spark, path)
