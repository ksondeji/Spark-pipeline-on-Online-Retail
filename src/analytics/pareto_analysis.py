from pyspark.sql import functions as F, Window

def _cumulative_contribution(df, group_col, value_col="total_revenue"):
    total = df.agg(F.sum(value_col)).first()[0]
    w = Window.orderBy(F.desc(value_col)).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        df.withColumn("cumulative_revenue", F.sum(value_col).over(w))
          .withColumn("cumulative_pct", F.round(F.col("cumulative_revenue") / total * 100, 2))
          .withColumn("rank", F.row_number().over(Window.orderBy(F.desc(value_col))))
    )

def get_customer_pareto(spark, gold_path):
    """% de clients qui génèrent 80% du CA."""
    df = spark.read.format("delta").load(gold_path)
    by_customer = (
        df.groupBy("CustomerID")
          .agg(F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("total_revenue"))
    )
    result = _cumulative_contribution(by_customer, "CustomerID")

    nb_customers = result.count()
    customers_for_80pct = result.filter(F.col("cumulative_pct") <= 80).count()
    pct_customers_for_80pct = round(customers_for_80pct / nb_customers * 100, 1)
    # ce chiffre (ex: "18% des clients font 80% du CA") est celui à mettre en avant en slide

    return result

def get_product_pareto(spark, gold_path):
    df = spark.read.format("delta").load(gold_path)
    by_product = (
        df.groupBy("ItemCode", "Description")
          .agg(F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("total_revenue"))
    )
    return _cumulative_contribution(by_product, "ItemCode")

def get_top_products(spark, gold_path, n=15):
    """Top produits par CA ET par volume — deux classements différents, souvent instructifs."""
    df = spark.read.format("delta").load(gold_path)
    by_revenue = (
        df.groupBy("ItemCode", "Description")
          .agg(F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("total_revenue"))
          .orderBy(F.desc("total_revenue"))
          .limit(n)
    )
    by_volume = (
        df.groupBy("ItemCode", "Description")
          .agg(F.sum("Quantity").alias("total_quantity"))
          .orderBy(F.desc("total_quantity"))
          .limit(n)
    )
    return by_revenue, by_volume