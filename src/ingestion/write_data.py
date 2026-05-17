from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
) -> None:
    """Écrit un DataFrame au format Delta Lake sur un chemin local ou cloud."""
    if not path.startswith(("s3://", "s3a://", "abfss://", "gs://")):
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
