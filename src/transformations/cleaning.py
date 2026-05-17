from pyspark.sql.functions import col, lower, to_timestamp

def clean_transactions(df):
    return (
        df.filter(col("CustomerID").isNotNull())
        .filter(col("UnitPrice").isNotNull())
        .filter(col("Quantity").isNotNull())
        .filter(col("InvoiceNo").isNotNull())
        # DELETE ... WHERE lower(InvoiceNo) LIKE 'c%'
        .filter(~lower(col("InvoiceNo")).startswith("c"))
        # DELETE ... WHERE InvoiceNo = '541431'
        .filter(col("InvoiceNo") != "541431")
        .filter(col("StockcCode").length() == 5)
        .withColumn("Quantity", col("Quantity").cast("int"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss"))
    )