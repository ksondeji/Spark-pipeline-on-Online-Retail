# continent ; segmentation ; catégories produits ; shopsize
from pyspark.sql.functions import col, when, lower

def enrich_transactions(df):
    return (
        df.withColumnRenamed("StockCode", "ItemCode")
        .withColumn("desc_clean", lower(col("Description")))
        .withColumn("OrderAmount", col("Quantity") * col("UnitPrice"))
        .withColumn("Purchase_segment", when(col("CustomerID") < 10000, "High_spender")
        .when(col("CustomerID") < 20000, "Medium_spender")
        .otherwise("Low_spender"))
        .withColumn("Shopsize", when(col("Country") == "United Kingdom", "Large")
        .when(col("Country") == "France", "Medium")
        .otherwise("Small"))
        .withColumn("Continent", when(col("Country") == "United Kingdom", "Europe")
        .when(col("Country") == "France", "Europe")
        .otherwise("Other"))
        .withColumn("product_category", when(col("desc_clean").contains("clock"), "Clocks")
        .when(col("desc_clean").contains("bag"), "Bags")
        .when(col("desc_clean").contains("heart"), "Hearts object")
        .when(col("desc_clean").contains("retrospot"), "Retrospots")
        .when(col("desc_clean").contains("cake"), "Cakes")
        .otherwise("Others"))
)