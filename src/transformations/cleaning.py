from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lower, to_timestamp, try_cast

# Filtres sur les colonnes encore en string (avant cast) — évite CAST_INVALID_INPUT
# quand une description (ex. "METAL SIGN") se retrouve dans Quantity / UnitPrice.
_QUANTITY_PATTERN = r"^-?\d+$"
_UNIT_PRICE_PATTERN = r"^-?\d+(\.\d+)?$"
_INVOICE_DATE_PATTERN = r"^\d{1,2}/\d{1,2}/\d{4}"


def clean_transactions(df: DataFrame) -> DataFrame:
    quantity_as_str = col("Quantity").cast("string")
    unit_price_as_str = col("UnitPrice").cast("string")
    invoice_date_as_str = col("InvoiceDate").cast("string")

    return (
        df.filter(col("CustomerID").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("Quantity").isNotNull())
        .filter(col("InvoiceNo").isNotNull())
        .filter(~lower(col("InvoiceNo")).startswith("c"))
        .filter(col("InvoiceNo") != "541431")
        .filter(length(col("StockCode")) == 5)
        # Exclure les lignes CSV mal alignées (texte dans colonnes numériques/dates)
        .filter(quantity_as_str.rlike(_QUANTITY_PATTERN))
        .filter(unit_price_as_str.rlike(_UNIT_PRICE_PATTERN))
        .filter(invoice_date_as_str.rlike(_INVOICE_DATE_PATTERN))
        .withColumn("Quantity", try_cast(col("Quantity"), "int"))
        .withColumn("UnitPrice", try_cast(col("UnitPrice"), "double"))
        .withColumn(
            "InvoiceDate",
            to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss"),
        )
        .filter(col("Quantity").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("InvoiceDate").isNotNull())
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") > 0)
    )
