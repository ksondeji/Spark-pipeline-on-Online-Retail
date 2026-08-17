# continent ; segmentation ; catégories produits ; shopsize
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, lower, when

from src.transformations.geo_mapping import COUNTRY_CONTINENT_CONTAINS


def _continent_from_country(country_col: Column = col("Country")) -> Column:
    """Mapping notebook : Country.contains(...) → Europa, Asia, etc."""
    if not COUNTRY_CONTINENT_CONTAINS:
        return lit("Other")

    first_country, first_continent = COUNTRY_CONTINENT_CONTAINS[0]
    result = when(country_col.contains(first_country), lit(first_continent))
    for country, continent in COUNTRY_CONTINENT_CONTAINS[1:]:
        result = result.when(country_col.contains(country), lit(continent))
    return result.otherwise(lit("Other"))


def enrich_transactions(df):
    order_amount = col("Quantity") * col("UnitPrice")

    return (
        df.withColumnRenamed("StockCode", "ItemCode")
        .withColumn("desc_clean", lower(col("Description")))
        .withColumn("OrderAmount", order_amount)
        # Notebook : segments par montant de ligne, pas par CustomerID
        .withColumn(
            "Purchase_segment",
            when(order_amount < lit(5), lit("Low"))
            .when(order_amount < lit(20), lit("Medium"))
            .otherwise(lit("High")),
        )
        .withColumn(
            "High_spender",
            (order_amount > lit(100)).cast("boolean"),
        )
        # Notebook : taille de commande via Quantity
        .withColumn(
            "Shopsize",
            when(col("Quantity") < lit(3), lit("Small"))
            .when(col("Quantity") < lit(12), lit("Medium"))
            .when(col("Quantity") < lit(50), lit("Big"))
            .otherwise(lit("Very_Big")),
        )
        .withColumn("Continent", _continent_from_country())
        .withColumn(
            "product_category",
            when(col("desc_clean").contains("clock"), lit("Clocks"))
            .when(col("desc_clean").contains("bag"), lit("Bags"))
            .when(col("desc_clean").contains("heart"), lit("Hearts object"))
            .when(col("desc_clean").contains("retrospot"), lit("Retrospots"))
            .when(col("desc_clean").contains("cake"), lit("Cakes"))
            .otherwise(lit("Others")),
        )
    )
