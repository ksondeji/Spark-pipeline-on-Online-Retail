"""
Enregistrement des tables Delta nommées comme dans le notebook d'origine (phase1…phase4).

Sur Databricks Unity Catalog : tables gérées sous ``{catalog}.{schema}.phase*``.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

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


def _table_path(base: str, name: str) -> str:
    return f"{base.rstrip('/')}/{name}"


def _delta_versions(spark: SparkSession, table_fqn: str) -> list[int]:
    rows = spark.sql(f"DESCRIBE HISTORY {table_fqn}").select("version").collect()
    return sorted({int(r["version"]) for r in rows})


def register_external_delta_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    name: str,
    path: str,
) -> str:
    """Crée ou met à jour une table UC pointant vers un emplacement Delta existant."""
    fqn = full_table_name(catalog, schema, name)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    spark.sql(
        f"""
        CREATE TABLE {fqn}
        USING DELTA
        LOCATION '{path}'
        """
    )
    logger.info("Table enregistrée : %s → %s", fqn, path)
    return fqn


def materialize_phase1(
    spark: SparkSession,
    silver_path: str,
    phase1_path: str,
    catalog: str,
    schema: str,
) -> str:
    """Silver + OrderAmount (équivalent notebook phase1 pour le time travel)."""
    phase1_df = (
        spark.read.format("delta")
        .load(silver_path)
        .withColumn("OrderAmount", col("Quantity") * col("UnitPrice"))
    )
    phase1_df.write.format("delta").mode("overwrite").save(phase1_path)
    return register_external_delta_table(
        spark, catalog, schema, "phase1", phase1_path
    )


def materialize_phase_from_gold(
    spark: SparkSession,
    gold_path: str,
    target_path: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> str:
    gold_df = spark.read.format("delta").load(gold_path)
    gold_df.write.format("delta").mode("overwrite").save(target_path)
    return register_external_delta_table(
        spark, catalog, schema, table_name, target_path
    )


def ensure_phase4_merge_history(
    spark: SparkSession,
    gold_path: str,
    phase4_path: str,
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

    window = Window.orderBy("InvoiceNo", "ItemCode", "CustomerID")
    ranked = gold_df.withColumn("_rn", row_number().over(window))
    seed = ranked.filter(col("_rn") <= seed_rows).drop("_rn")
    rest = ranked.filter(col("_rn") > seed_rows).drop("_rn")

    seed.write.format("delta").mode("overwrite").save(phase4_path)
    register_external_delta_table(spark, catalog, schema, "phase4", phase4_path)

    rest.createOrReplaceTempView("df_rest_phase5")
    merge_fqn = full_table_name(catalog, schema, "df_rest_phase5")
    spark.sql(f"DROP TABLE IF EXISTS {merge_fqn}")
    (
        rest.write.format("delta")
        .mode("overwrite")
        .saveAsTable(merge_fqn)
    )

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


def register_phase4_from_gold_if_exists(
    spark: SparkSession,
    gold_path: str,
    phase4_path: str,
    catalog: str,
    schema: str,
) -> str:
    """Enregistre phase4 depuis gold sans recréer l'historique MERGE."""
    return materialize_phase_from_gold(
        spark, gold_path, phase4_path, catalog, schema, "phase4"
    )


def create_partitioned_analytics_tables(
    spark: SparkSession,
    phase4_fqn: str,
    catalog: str,
    schema: str,
) -> list[str]:
    """Tables partitionnées du notebook (format Delta sur UC)."""
    created: list[str] = []
    for table_name, partition_cols in _PARTITIONED_TABLES:
        fqn = full_table_name(catalog, schema, table_name)
        cols = ", ".join(partition_cols)
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        spark.sql(
            f"""
            CREATE TABLE {fqn}
            USING DELTA
            PARTITIONED BY ({cols})
            AS SELECT * FROM {phase4_fqn}
            """
        )
        logger.info("Table partitionnée : %s BY (%s)", fqn, cols)
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
    base = tables_cfg["base"]

    phase1_path = _table_path(base, "phase1")
    phase2_path = _table_path(base, "phase2")
    phase3_path = _table_path(base, "phase3")
    phase4_path = _table_path(base, "phase4")

    registered: dict[str, str] = {}

    registered["phase1"] = materialize_phase1(
        spark, paths["silver"], phase1_path, catalog, schema
    )
    # phase2 notebook ≈ phase1 + OrderAmount (avant enrichissements lourds)
    (
        spark.read.format("delta")
        .load(phase1_path)
        .write.format("delta")
        .mode("overwrite")
        .save(phase2_path)
    )
    registered["phase2"] = register_external_delta_table(
        spark, catalog, schema, "phase2", phase2_path
    )

    registered["phase3"] = materialize_phase_from_gold(
        spark, paths["gold"], phase3_path, catalog, schema, "phase3"
    )

    phase4_fqn = full_table_name(catalog, schema, "phase4")
    if simulate_phase4_merge:
        registered["phase4"] = ensure_phase4_merge_history(
            spark,
            paths["gold"],
            phase4_path,
            catalog,
            schema,
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
            registered["phase4"] = register_phase4_from_gold_if_exists(
                spark, paths["gold"], phase4_path, catalog, schema
            )

    create_partitioned_analytics_tables(
        spark, registered["phase4"], catalog, schema
    )
    for table_name, _ in _PARTITIONED_TABLES:
        registered[table_name] = full_table_name(catalog, schema, table_name)

    return registered
