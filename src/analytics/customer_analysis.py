from pyspark.sql import Window
from pyspark.sql import functions as F


def get_rfm_table(spark, gold_path, reference_date=None):
    """
    Recency / Frequency / Monetary par client, avec scoring quartile (1=faible, 4=fort)
    et segment métier dérivé.
    """
    df = spark.read.format("delta").load(gold_path)

    if reference_date is None:
        reference_date = df.agg(F.max("InvoiceDate")).first()[0]

    rfm = df.groupBy("CustomerID").agg(
        F.datediff(F.lit(reference_date), F.max("InvoiceDate")).alias(
            "recency_days"
        ),
        F.countDistinct("InvoiceNo").alias("frequency"),
        F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("monetary"),
    )

    # Scoring en quartiles (4 = meilleur, sauf recency où c'est inversé)
    def quartile_score(col_name, ascending=True):
        order = (
            F.col(col_name).asc() if ascending else F.col(col_name).desc()
        )
        w = Window.orderBy(order)
        return F.ntile(4).over(w)

    rfm = (
        rfm.withColumn(
            "r_score", quartile_score("recency_days", ascending=False)
        )
        .withColumn("f_score", quartile_score("frequency", ascending=True))
        .withColumn("m_score", quartile_score("monetary", ascending=True))
        .withColumn(
            "rfm_score",
            F.col("r_score") + F.col("f_score") + F.col("m_score"),
        )
    )

    rfm = rfm.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 10, "Champions")
        .when(F.col("rfm_score") >= 8, "Loyal")
        .when(F.col("rfm_score") >= 6, "Potentiel")
        .when(F.col("rfm_score") >= 4, "A risque")
        .otherwise("Perdu"),
    )
    return rfm


def get_rfm_segment_summary(spark, gold_path):
    """Nombre de clients et CA par segment RFM — synthèse pour la présentation."""
    rfm = get_rfm_table(spark, gold_path)
    return (
        rfm.groupBy("rfm_segment")
        .agg(
            F.count("*").alias("nb_customers"),
            F.sum("monetary").alias("total_revenue"),
            F.avg("monetary").alias("avg_customer_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def get_cohort_retention(spark, gold_path):
    """
    Rétention par cohorte d'acquisition (mois du 1er achat) :
    % de clients actifs à M+0, M+1, M+2...
    """
    df = spark.read.format("delta").load(gold_path)

    first_purchase = (
        df.groupBy("CustomerID")
        .agg(F.min("InvoiceDate").alias("first_purchase_date"))
        .withColumn(
            "cohort_month", F.date_format("first_purchase_date", "yyyy-MM")
        )
    )

    activity = df.select("CustomerID", "InvoiceDate").join(
        first_purchase, "CustomerID"
    )

    activity = activity.withColumn(
        "period_number",
        F.months_between(
            F.to_date(F.date_format("InvoiceDate", "yyyy-MM-01")),
            F.to_date(F.date_format("first_purchase_date", "yyyy-MM-01")),
        ).cast("int"),
    )

    cohort_size = first_purchase.groupBy("cohort_month").agg(
        F.count("*").alias("cohort_size")
    )

    retention = (
        activity.groupBy("cohort_month", "period_number")
        .agg(F.countDistinct("CustomerID").alias("active_customers"))
        .join(cohort_size, "cohort_month")
        .withColumn(
            "retention_rate",
            F.round(
                F.col("active_customers") / F.col("cohort_size") * 100, 1
            ),
        )
        .orderBy("cohort_month", "period_number")
    )
    return retention
