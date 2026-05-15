from pyspark.sql import SparkSession

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("online-retail-pipeline")
        .config(...)
        .getOrCreate()
    )

    return spark