"""
Orchestration du pipeline Online Retail (bronze → silver → gold).

Local :
  python -m src.main [--env dev] [--analytics]

Databricks (job ou notebook, repo attaché au cluster) :
  python -m src.main --env databricks [--analytics]
  # ou sans --env : détection auto (ENV=databricks)
"""

from __future__ import annotations

import argparse
import sys

from src.analytics.runner import run_analytics
from src.ingestion.read_data import read_raw_csv
from src.ingestion.spark_session import get_spark
from src.ingestion.write_data import write_delta
from src.quality.checks import run_checks
from src.transformations.cleaning import clean_transactions
from src.transformations.enrichment import enrich_transactions
from src.utils.config import get_config, is_databricks_cluster
from src.utils.logger import logger


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = get_config(args.env)
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
        logger.info("Lignes brutes ingérées : %s", df_raw.count())

        write_delta(df_raw, paths["bronze"])
        logger.info("Bronze écrit : %s", paths["bronze"])

        df_silver = clean_transactions(df_raw)
        logger.info("Lignes après nettoyage : %s", df_silver.count())

        quality_report = run_checks(df_silver, scope="cleaning", raise_on_failure=True)
        logger.info("Contrôles silver OK (%s lignes)", quality_report["row_count"])

        write_delta(df_silver, paths["silver"])
        logger.info("Silver écrit : %s", paths["silver"])

        df_gold = enrich_transactions(df_silver)
        gold_report = run_checks(df_gold, scope="enriched", raise_on_failure=True)
        logger.info("Contrôles gold OK (%s lignes)", gold_report["row_count"])

        write_delta(df_gold, paths["gold"])
        logger.info("Gold écrit : %s", paths["gold"])

        if args.analytics:
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


if __name__ == "__main__":
    sys.exit(main())
