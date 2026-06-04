"""
Orchestration du pipeline Online Retail (bronze → silver → gold).

Local :
  python -m src.main [--env dev] [--analytics]

Databricks notebook (repo attaché au cluster) :
  from src.main import run_pipeline
  run_pipeline(analytics=True)  # env auto → config/databricks.yaml

Databricks (job / terminal) :
  python -m src.main [--env databricks] [--analytics]
"""

from __future__ import annotations

import argparse
import sys

from src.analytics.runner import run_analytics
from src.ingestion.read_data import read_raw_csv
from src.ingestion.spark_session import get_spark
from src.ingestion.write_data import read_delta, write_delta
from src.quality.checks import run_checks
from pyspark.sql.functions import col

from src.transformations.cleaning import clean_transactions
from src.transformations.enrichment import enrich_transactions
from src.utils.config import get_config, is_databricks_cluster
from src.utils.logger import logger
from src.utils.schema_debug import inspect_dataframe_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline Online Retail")
    parser.add_argument(
        "--env",
        default=None,
        help="Fichier config à charger : dev (local), databricks, prod (S3/cluster). "
        "Sur Databricks, la valeur par défaut est 'databricks'.",
    )
    parser.add_argument(
        "--analytics",
        action="store_true",
        help="Lance les analyses et le benchmark de partitionnement après l'écriture gold.",
    )
    parser.add_argument(
        "--debug-schema",
        action="store_true",
        help="Affiche schéma et échantillons après raw / silver / gold (diagnostic colonnes).",
    )
    return parser.parse_args()


def run_pipeline(
    env: str | None = None,
    *,
    analytics: bool = False,
    debug_schema: bool = False,
) -> int:
    """
    Point d'entrée pour notebook Databricks (évite argparse / sys.argv).

    Sur un cluster attaché, ``env`` peut être omis : détection auto → config/databricks.yaml.
    La session ``spark`` du notebook est réutilisée (pas de cluster_id requis).
    """
    config = get_config(env)
    paths = config["paths"]
    on_db_cluster = config["is_databricks"]

    logger.info("Runtime : %s | env=%s", config["runtime"], config["env"])
    logger.info("RAW    → %s", paths["raw"])
    logger.info("BRONZE → %s", paths["bronze"])
    logger.info("SILVER → %s", paths["silver"])
    logger.info("GOLD   → %s", paths["gold"])

    spark = None
    try:
        spark = get_spark(config)

        df_raw = read_raw_csv(spark, paths["raw"])
        raw_count = df_raw.count()
        corrupt_count = (
            df_raw.filter(col("_corrupt_record").isNotNull()).count()
            if "_corrupt_record" in df_raw.columns
            else 0
        )
        logger.info(
            "Lignes brutes ingérées : %s (%s corrompues isolées)",
            raw_count,
            corrupt_count,
        )
        if debug_schema:
            inspect_dataframe_schema(df_raw, "raw (après lecture CSV)")

        write_delta(df_raw, paths["bronze"])
        logger.info("Bronze écrit : %s", paths["bronze"])

        df_silver = clean_transactions(df_raw)
        logger.info("Lignes après nettoyage : %s", df_silver.count())

        write_delta(df_silver, paths["silver"])
        logger.info("Silver écrit : %s", paths["silver"])

        # Relire depuis Delta : casse la lignée CSV (évite CAST_INVALID_INPUT dans checks)
        df_silver = read_delta(spark, paths["silver"])
        if debug_schema:
            inspect_dataframe_schema(df_silver, "silver (Delta relu)")

        quality_report = run_checks(df_silver, scope="silver", raise_on_failure=True)
        logger.info("Contrôles silver OK (%s lignes)", quality_report["row_count"])

        df_gold = enrich_transactions(df_silver)

        write_delta(df_gold, paths["gold"])
        logger.info("Gold écrit : %s", paths["gold"])

        df_gold = read_delta(spark, paths["gold"])
        if debug_schema:
            inspect_dataframe_schema(df_gold, "gold (Delta relu)")

        gold_report = run_checks(df_gold, scope="enriched", raise_on_failure=True)
        logger.info("Contrôles gold OK (%s lignes)", gold_report["row_count"])

        if analytics:
            run_analytics(spark, paths["gold"])

        logger.info("Pipeline terminé avec succès.")
        return 0

    except Exception:
        logger.exception("Échec du pipeline")
        return 1

    finally:
        # Ne pas arrêter la session partagée du cluster Databricks
        if spark is not None and not on_db_cluster:
            spark.stop()


def main() -> int:
    args = parse_args()
    return run_pipeline(
        args.env,
        analytics=args.analytics,
        debug_schema=args.debug_schema,
    )


if __name__ == "__main__":
    sys.exit(main())
