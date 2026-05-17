from __future__ import annotations

from typing import Any

from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_spark_session(spark_config: dict[str, Any] | None = None) -> SparkSession:
    """Crée une SparkSession locale avec l'extension Delta Lake."""
    spark_config = spark_config or {}
    conf = (
        SparkConf()
        .setAppName(spark_config.get("app_name", "OnlineRetail-pipeline"))
        .setMaster(spark_config.get("master", "local[*]"))
        .set("spark.driver.memory", spark_config.get("driver_memory", "4G"))
        .set("spark.executor.memory", spark_config.get("executor_memory", "4G"))
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
