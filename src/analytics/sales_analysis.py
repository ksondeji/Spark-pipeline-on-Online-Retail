#  Fonctions qui prennent un SparkSession + chemin gold, retournent des DataFrames
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, mean, count
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

def get_sales_by_country(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Country").agg(sum("OrderAmount").alias("total_revenue"))

def get_sales_by_product_category(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("product_category").agg(sum("OrderAmount").alias("total_revenue"))

def get_sales_by_continent(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Continent").agg(sum("OrderAmount").alias("total_revenue"))

def get_sales_by_purchase_segment(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Purchase_segment").agg(sum("OrderAmount").alias("total_revenue"))

def get_sales_by_shopsize(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Shopsize").agg(sum("OrderAmount").alias("total_revenue"))

def get_mean_spend_by_segment(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Purchase_segment").agg(mean("OrderAmount").alias("mean_spend"))

def get_top_category_for_big_customer(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).filter(col("High_spender") == True).groupBy("product_category").agg(count("*").alias("number_of_purchase")).orderBy(col("number_of_purchase").desc()).limit(10)

def get_sales_by_country_continent(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Country", "Continent").agg(sum("OrderAmount").alias("total_revenue")).orderBy(col("total_revenue").desc())

def get_cumulative_contribution_by_country(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("Country").agg(sum("OrderAmount").alias("total_revenue")).orderBy(col("total_revenue").desc()).withColumn("cumulative_contribution", sum("total_revenue").over(Window.orderBy(col("total_revenue").desc())))

def get_cumulative_contribution_by_customer_id(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path).groupBy("CustomerID").agg(sum("OrderAmount").alias("total_revenue")).orderBy(col("total_revenue").desc()).withColumn("cumulative_contribution", sum("total_revenue").over(Window.orderBy(col("total_revenue").desc()))).withColumn("cumulative_percentage", col("cumulative_contribution") / col("total_revenue"))
