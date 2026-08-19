from pyspark.sql import functions as F

def get_returns_summary(spark, gold_path):
    """Vue globale : part du CA annulé, nombre de transactions concernées."""
    df = spark.read.format("delta").load(gold_path)

    df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))

    return (
        df.groupBy("is_return")
          .agg(
              F.count("*").alias("nb_lines"),
              F.countDistinct("InvoiceNo").alias("nb_invoices"),
              F.sum(F.abs(F.col("Quantity") * F.col("UnitPrice"))).alias("total_amount"),
          )
    )

def get_return_rate_by_category(spark, gold_path):
    """Taux de retour par catégorie de produit — identifie les catégories problématiques."""
    df = spark.read.format("delta").load(gold_path)
    df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))

    return (
        df.groupBy("product_category")
          .agg(
              F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("nb_returns"),
              F.count("*").alias("nb_total"),
          )
          .withColumn("return_rate_pct", F.round(F.col("nb_returns") / F.col("nb_total") * 100, 2))
          .orderBy(F.desc("return_rate_pct"))
    )

def get_return_rate_by_country(spark, gold_path):
    df = spark.read.format("delta").load(gold_path)
    df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))

    return (
        df.groupBy("Country")
          .agg(
              F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("nb_returns"),
              F.count("*").alias("nb_total"),
          )
          .withColumn("return_rate_pct", F.round(F.col("nb_returns") / F.col("nb_total") * 100, 2))
          .orderBy(F.desc("return_rate_pct"))
    )