from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_REMOTE_PREFIXES = ("s3://", "s3a://", "abfss://", "gs://", "dbfs:", "/Volumes/")


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
    *,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
) -> None:
    """Écrit un DataFrame au format Delta Lake sur un chemin local ou cloud."""
    if not path.startswith(_REMOTE_PREFIXES):
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    writer = df.write.format("delta").mode(mode)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def drop_corrupt_column(df: DataFrame) -> DataFrame:
    """Retire _corrupt_record avant persistance bronze (schéma métier stable)."""
    if "_corrupt_record" in df.columns:
        return df.drop("_corrupt_record")
    return df


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Relit une table Delta matérialisée (casse la lignée Spark issue du CSV)."""
    return spark.read.format("delta").load(path)
