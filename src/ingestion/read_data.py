from pyspark.sql.types import *

online_retail_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    ...
])

def load_raw_data(spark, path):
    return (
        spark.read
        .option("header", True)
        .schema(online_retail_schema)
        .csv(path)
    )