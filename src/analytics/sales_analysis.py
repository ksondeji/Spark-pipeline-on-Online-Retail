from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, mean, sum
from pyspark.sql.window import Window


def _read_gold(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def get_sales_by_country(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("Country").agg(
        sum("OrderAmount").alias("total_revenue")
    )


def get_sales_by_product_category(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("product_category").agg(
        sum("OrderAmount").alias("total_revenue")
    )


def get_sales_by_continent(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("Continent").agg(
        sum("OrderAmount").alias("total_revenue")
    )


def get_sales_by_purchase_segment(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("Purchase_segment").agg(
        sum("OrderAmount").alias("total_revenue")
    )


def get_sales_by_shopsize(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("Shopsize").agg(
        sum("OrderAmount").alias("total_revenue")
    )


def get_mean_spend_by_segment(spark: SparkSession, path: str) -> DataFrame:
    return _read_gold(spark, path).groupBy("Purchase_segment").agg(
        mean("OrderAmount").alias("mean_spend")
    )


def get_top_category_for_big_customer(spark: SparkSession, path: str) -> DataFrame:
    return (
        _read_gold(spark, path)
        .filter(col("Purchase_segment") == "High_spender")
        .groupBy("product_category")
        .agg(count("*").alias("number_of_purchase"))
        .orderBy(col("number_of_purchase").desc())
        .limit(10)
    )


def get_sales_by_country_continent(spark: SparkSession, path: str) -> DataFrame:
    return (
        _read_gold(spark, path)
        .groupBy("Country", "Continent")
        .agg(sum("OrderAmount").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )


def get_cumulative_contribution_by_country(spark: SparkSession, path: str) -> DataFrame:
    window = Window.orderBy(col("total_revenue").desc())
    return (
        _read_gold(spark, path)
        .groupBy("Country")
        .agg(sum("OrderAmount").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .withColumn("cumulative_contribution", sum("total_revenue").over(window))
    )


def get_cumulative_contribution_by_customer_id(
    spark: SparkSession, path: str
) -> DataFrame:
    window = Window.orderBy(col("total_revenue").desc())
    return (
        _read_gold(spark, path)
        .groupBy("CustomerID")
        .agg(sum("OrderAmount").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .withColumn("cumulative_contribution", sum("total_revenue").over(window))
    )