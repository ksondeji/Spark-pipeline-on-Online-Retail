from pyspark.sql import functions as F


def _product_category_dim(spark, gold_path):
    return (
        spark.read.format("delta")
        .load(gold_path)
        .select("ItemCode", "Description", "product_category")
        .distinct()
    )


def _bronze_with_category(spark, bronze_path, gold_path):
    cat_dim = _product_category_dim(spark, gold_path)
    bronze = spark.read.format("delta").load(bronze_path)
    return (
        bronze.alias("b")
        .join(
            cat_dim.alias("c"),
            (F.col("b.StockCode") == F.col("c.ItemCode"))
            & (F.col("b.Description") == F.col("c.Description")),
            "left",
        )
        .select("b.*", F.col("c.product_category"))
    )


def _is_return_line():
    qty = F.expr("try_cast(Quantity AS int)")
    return F.lower(F.col("InvoiceNo")).startswith("c") | (qty < 0)


def get_returns_summary(spark, gold_path, bronze_path=None):
    """Vue globale : part du CA annulé (retours lus depuis bronze si fourni)."""
    if not bronze_path:
        df = spark.read.format("delta").load(gold_path)
        df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))
    else:
        df = _bronze_with_category(spark, bronze_path, gold_path).withColumn(
            "is_return", _is_return_line()
        )

    return df.groupBy("is_return").agg(
        F.count("*").alias("nb_lines"),
        F.countDistinct("InvoiceNo").alias("nb_invoices"),
        F.sum(
            F.abs(F.expr("try_cast(Quantity AS int)") * F.expr("try_cast(UnitPrice AS double)"))
        ).alias("total_amount"),
    )


def get_return_rate_by_category(spark, gold_path, bronze_path=None):
    """
    Taux de retour par catégorie (% lignes retour / lignes retour + ventes).

    Les annulations ne sont pas dans gold : passer ``bronze_path`` (couche brute).
    """
    if bronze_path is None:
        df = spark.read.format("delta").load(gold_path)
        df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))
        return (
            df.groupBy("product_category")
            .agg(
                F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("nb_returns"),
                F.count("*").alias("nb_total"),
            )
            .withColumn(
                "return_rate_pct",
                F.round(F.col("nb_returns") / F.col("nb_total") * 100, 2),
            )
            .orderBy(F.desc("return_rate_pct"))
        )

    bronze_labeled = _bronze_with_category(spark, bronze_path, gold_path)
    returns = (
        bronze_labeled.filter(_is_return_line())
        .groupBy("product_category")
        .agg(F.count("*").alias("nb_returns"))
    )
    sales = (
        spark.read.format("delta")
        .load(gold_path)
        .groupBy("product_category")
        .agg(F.count("*").alias("nb_sales"))
    )
    return (
        sales.join(returns, "product_category", "outer")
        .fillna(0, subset=["nb_returns", "nb_sales"])
        .withColumn("nb_total", F.col("nb_returns") + F.col("nb_sales"))
        .withColumn(
            "return_rate_pct",
            F.round(F.col("nb_returns") / F.col("nb_total") * 100, 2),
        )
        .filter(F.col("product_category").isNotNull())
        .filter(F.col("nb_total") > 0)
        .orderBy(F.desc("return_rate_pct"))
    )


def get_return_rate_by_country(spark, gold_path, bronze_path=None):
    if not bronze_path:
        df = spark.read.format("delta").load(gold_path)
        df = df.withColumn("is_return", F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0))
    else:
        df = _bronze_with_category(spark, bronze_path, gold_path).withColumn(
            "is_return", _is_return_line()
        )

    return (
        df.groupBy("Country")
        .agg(
            F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("nb_returns"),
            F.count("*").alias("nb_total"),
        )
        .withColumn(
            "return_rate_pct",
            F.round(F.col("nb_returns") / F.col("nb_total") * 100, 2),
        )
        .orderBy(F.desc("return_rate_pct"))
    )
