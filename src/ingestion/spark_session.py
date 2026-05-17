from pyspark.sql import SparkSession

def create_spark_session():
#"""Extrait exactement le bloc de création de SparkSession de ton notebook. La config Delta Lake, les jars, tout. Prend la config en paramètre depuis config.py"""
    spark = (
        SparkSession.builder
        .appName("online-retail-pipeline")
        .config(...)
        .getOrCreate()
    )

    return spark