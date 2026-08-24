from pyspark.sql import functions as F


def get_monthly_revenue(spark, gold_path):
    """CA mensuel : tendance globale sur la période."""
    df = spark.read.format("delta").load(gold_path)
    return (
        df.withColumn("year_month", F.date_format("InvoiceDate", "yyyy-MM"))
        .groupBy("year_month")
        .agg(
            F.sum(F.col("Quantity") * F.col("UnitPrice")).alias(
                "total_revenue"
            ),
            F.countDistinct("InvoiceNo").alias("nb_orders"),
            F.countDistinct("CustomerID").alias("nb_active_customers"),
        )
        .orderBy("year_month")
    )


def get_weekday_pattern(spark, gold_path):
    """Saisonnalité hebdomadaire : quel jour vend le plus."""
    df = spark.read.format("delta").load(gold_path)
    return (
        df.withColumn("weekday", F.date_format("InvoiceDate", "EEEE"))
        .groupBy("weekday")
        .agg(
            F.sum(F.col("Quantity") * F.col("UnitPrice")).alias(
                "total_revenue"
            ),
            F.count("InvoiceNo").alias("nb_transactions"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def get_hourly_pattern(spark, gold_path):
    """Distribution horaire des achats (si heure dans InvoiceDate)."""
    df = spark.read.format("delta").load(gold_path)
    return (
        df.withColumn("hour", F.hour("InvoiceDate"))
        .groupBy("hour")
        .agg(
            F.sum(F.col("Quantity") * F.col("UnitPrice")).alias(
                "total_revenue"
            )
        )
        .orderBy("hour")
    )
