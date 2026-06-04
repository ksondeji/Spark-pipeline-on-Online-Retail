from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, length, lower, to_timestamp, try_cast


def clean_transactions(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("CustomerID").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("Quantity").isNotNull())
        .filter(col("InvoiceNo").isNotNull())
        .filter(~lower(col("InvoiceNo")).startswith("c"))
        .filter(col("InvoiceNo") != "541431")
        .filter(length(col("StockCode")) == 5)
        .withColumn("Quantity", try_cast(col("Quantity"), "int"))
        .withColumn("UnitPrice", try_cast(col("UnitPrice"), "double"))
        .withColumn(
            "InvoiceDate",
            to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss"),
        )
        # Lignes dont le cast échoue (ex. date dans UnitPrice) — tolérant Databricks ANSI
        .filter(col("Quantity").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("InvoiceDate").isNotNull())
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") > 0)
    )
