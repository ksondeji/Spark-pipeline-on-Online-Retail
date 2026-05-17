from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from src.analytics.performance_analysis import benchmark_partitioned_vs_non_partitioned
from src.analytics.sales_analysis import (
    get_sales_by_continent,
    get_sales_by_country,
    get_sales_by_product_category,
    get_sales_by_purchase_segment,
)
from src.utils.logger import logger


def run_analytics(spark: SparkSession, gold_path: str) -> None:
    """Exécute les analyses ventes et un benchmark partitionnement."""
    logger.info("=== Analytique : CA par pays ===")
    get_sales_by_country(spark, gold_path).show(20, truncate=False)

    logger.info("=== Analytique : CA par catégorie produit ===")
    get_sales_by_product_category(spark, gold_path).show(20, truncate=False)

    logger.info("=== Analytique : CA par continent ===")
    get_sales_by_continent(spark, gold_path).show(truncate=False)

    logger.info("=== Analytique : CA par segment d'achat ===")
    get_sales_by_purchase_segment(spark, gold_path).show(truncate=False)

    _run_partition_benchmark(spark, gold_path)


def _run_partition_benchmark(spark: SparkSession, gold_path: str) -> None:
    """Crée des vues temporaires pour comparer lecture non partitionnée vs partitionnée."""
    gold_df = spark.read.format("delta").load(gold_path)
    gold_df.createOrReplaceTempView("phase4")

    partitioned_path = str(Path(gold_path).parent / "gold_partitioned_benchmark")
    (
        gold_df.write.format("delta")
        .partitionBy("Country", "Continent")
        .mode("overwrite")
        .save(partitioned_path)
    )
    spark.read.format("delta").load(partitioned_path).createOrReplaceTempView(
        "sales_per_country_continent"
    )

    bench = benchmark_partitioned_vs_non_partitioned(spark)
    logger.info(
        "Benchmark partitionnement — non partitionné: %.2fs | partitionné: %.2fs | gain: %s%%",
        bench["non_partitioned"]["elapsed_seconds"],
        bench["partitioned"]["elapsed_seconds"],
        f"{bench['speedup_pct']:.1f}" if bench["speedup_pct"] is not None else "n/a",
    )
