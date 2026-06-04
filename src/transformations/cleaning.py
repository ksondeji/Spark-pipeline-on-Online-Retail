from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lit, lower, to_timestamp, try_cast

_QUANTITY_PATTERN = r"^-?\d+$"
_UNIT_PRICE_PATTERN = r"^-?\d+(\.\d+)?$"
_INVOICE_DATE_PATTERN = r"^\d{1,2}/\d{1,2}/\d{4}(\s+\d{1,2}:\d{2}:\d{2})?$"


def clean_transactions(df: DataFrame) -> DataFrame:
    quantity_as_str = col("Quantity").cast("string")
    unit_price_as_str = col("UnitPrice").cast("string")
    invoice_date_as_str = col("InvoiceDate").cast("string")

    qty = try_cast(col("Quantity"), "int")
    price = try_cast(col("UnitPrice"), "double")

    return (
        df.filter(col("CustomerID").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("Quantity").isNotNull())
        .filter(col("InvoiceNo").isNotNull())
        .filter(~lower(col("InvoiceNo")).startswith("c"))
        .filter(col("InvoiceNo") != "541431")
        .filter(length(col("StockCode")) == 5)
        .filter(quantity_as_str.rlike(_QUANTITY_PATTERN))
        .filter(unit_price_as_str.rlike(_UNIT_PRICE_PATTERN))
        .filter(~unit_price_as_str.rlike(r"[/:]"))  # exclut dates dans UnitPrice
        .filter(invoice_date_as_str.rlike(_INVOICE_DATE_PATTERN))
        .withColumn("Quantity", qty)
        .withColumn("UnitPrice", price)
        .withColumn(
            "InvoiceDate",
            to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss"),
        )
        .filter(col("Quantity").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("InvoiceDate").isNotNull())
        .filter(col("Quantity") > lit(0))
        .filter(col("UnitPrice") > lit(0))
    )
