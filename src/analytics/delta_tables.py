"""
Enregistrement des tables Delta nommées comme dans le notebook d'origine (phase1…phase4).

Comptes étudiants Databricks : tables **gérées** UC via ``saveAsTable`` (pas de LOCATION
sur Volume, qui provoque ``Missing cloud file system scheme``).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.utils.logger import logger

# Taille initiale de phase4 avant MERGE dans le notebook (~5 322 lignes)
PHASE4_MERGE_SEED_ROWS = 5322

_PARTITIONED_TABLES: list[tuple[str, list[str]]] = [
    ("sales_per_country_continent", ["Country", "Continent"]),
    ("sales_clus_country_Shopsize", ["Country", "Shopsize"]),
    ("product_category_per_continent", ["product_category", "Continent"]),
]


def full_table_name(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def _ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _delta_versions(spark: SparkSession, table_fqn: str) -> list[int]:
    rows = spark.sql(f"DESCRIBE HISTORY {table_fqn}").select("version").collect()
    return sorted({int(r["version"]) for r in rows})


def _save_managed_table(
    df: DataFrame,
    catalog: str,
    schema: str,
    name: str,
    *,
    partition_cols: list[str] | None = None,
) -> str:
    """Écrit une table Delta gérée dans Unity Catalog (compatible compte étudiant)."""
    session = df.sparkSession
    _ensure_schema(session, catalog, schema)
    fqn = full_table_name(catalog, schema, name)
    session.sql(f"DROP TABLE IF EXISTS {fqn}")

    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(fqn)

    logger.info("Table gérée créée : %s", fqn)
    return fqn


def materialize_phase1(
    spark: SparkSession,
    silver_path: str,
    catalog: str,
    schema: str,
) -> str:
    """Silver + OrderAmount (équivalent notebook phase1 pour le time travel)."""
    phase1_df = (
        spark.read.format("delta")
        .load(silver_path)
        .withColumn("OrderAmount", col("Quantity") * col("UnitPrice"))
    )
    return _save_managed_table(phase1_df, catalog, schema, "phase1")


def materialize_phase_from_gold(
    spark: SparkSession,
    gold_path: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> str:
    gold_df = spark.read.format("delta").load(gold_path)
    return _save_managed_table(gold_df, catalog, schema, table_name)


def ensure_phase4_merge_history(
    spark: SparkSession,
    gold_path: str,
    catalog: str,
    schema: str,
    *,
    seed_rows: int = PHASE4_MERGE_SEED_ROWS,
) -> str:
    """
    Reproduit l'historique notebook : phase4 v0 (échantillon) puis MERGE v1 (reste).

    Équivalent de ``MERGE INTO phase4 USING df_rest_phase5``.
    """
    phase4_fqn = full_table_name(catalog, schema, "phase4")
    gold_df = spark.read.format("delta").load(gold_path)

    merge_keys = ["InvoiceNo", "ItemCode", "CustomerID"]
    ordered = gold_df.orderBy(*merge_keys)
    seed = ordered.limit(seed_rows)
    rest = gold_df.join(seed.select(*merge_keys), on=merge_keys, how="left_anti")

    _save_managed_table(seed, catalog, schema, "phase4")
    merge_fqn = _save_managed_table(rest, catalog, schema, "df_rest_phase5")

    spark.sql(
        f"""
        MERGE INTO {phase4_fqn} AS t
        USING {merge_fqn} AS s
        ON t.InvoiceNo = s.InvoiceNo
           AND t.ItemCode = s.ItemCode
           AND t.CustomerID = s.CustomerID
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    versions = _delta_versions(spark, phase4_fqn)
    logger.info(
        "Historique phase4 après MERGE : versions %s (%s lignes)",
        versions,
        spark.table(phase4_fqn).count(),
    )
    return phase4_fqn


def create_partitioned_analytics_tables(
    spark: SparkSession,
    phase4_fqn: str,
    catalog: str,
    schema: str,
) -> list[str]:
    """Tables partitionnées du notebook (Delta géré, partitionné)."""
    phase4_df = spark.table(phase4_fqn)
    created: list[str] = []
    for table_name, partition_cols in _PARTITIONED_TABLES:
        fqn = _save_managed_table(
            phase4_df,
            catalog,
            schema,
            table_name,
            partition_cols=partition_cols,
        )
        created.append(fqn)
    return created


def register_notebook_tables(
    spark: SparkSession,
    paths: dict[str, str],
    tables_cfg: dict[str, Any],
    *,
    simulate_phase4_merge: bool = True,
) -> dict[str, str]:
    """
    Enregistre phase1…phase4 et tables partitionnées (noms identiques au notebook).

    Returns:
        Dictionnaire nom logique → nom qualifié UC (ex. ``phase4`` → ``main.default.phase4``).
    """
    catalog = tables_cfg.get("catalog", "main")
    schema = tables_cfg.get("schema", "default")
    _ensure_schema(spark, catalog, schema)

    registered: dict[str, str] = {}

    registered["phase1"] = materialize_phase1(
        spark, paths["silver"], catalog, schema
    )
    registered["phase2"] = _save_managed_table(
        spark.table(registered["phase1"]),
        catalog,
        schema,
        "phase2",
    )
    registered["phase3"] = materialize_phase_from_gold(
        spark, paths["gold"], catalog, schema, "phase3"
    )

    phase4_fqn = full_table_name(catalog, schema, "phase4")
    if simulate_phase4_merge:
        registered["phase4"] = ensure_phase4_merge_history(
            spark, paths["gold"], catalog, schema
        )
    else:
        existing_versions: list[int] = []
        try:
            existing_versions = _delta_versions(spark, phase4_fqn)
        except Exception:
            pass
        if len(existing_versions) >= 2:
            registered["phase4"] = phase4_fqn
            logger.info("phase4 existante conservée (versions %s)", existing_versions)
        else:
            registered["phase4"] = materialize_phase_from_gold(
                spark, paths["gold"], catalog, schema, "phase4"
            )

    create_partitioned_analytics_tables(
        spark, registered["phase4"], catalog, schema
    )
    for table_name, _ in _PARTITIONED_TABLES:
        registered[table_name] = full_table_name(catalog, schema, table_name)

    return registered
