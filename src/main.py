"""
Orchestration du pipeline Online Retail (bronze → silver → gold).
Lancement : python -m src.main [--env dev] [--analytics]
"""

from __future__ import annotations

import argparse
import sys

from src.analytics.runner import run_analytics
from src.ingestion.read_data import read_raw_csv
from src.ingestion.spark_session import create_spark_session
from src.ingestion.write_data import write_delta
from src.quality.checks import run_checks
from src.transformations.cleaning import clean_transactions
from src.transformations.enrichment import enrich_transactions
from src.utils.config import get_config
from src.utils.logger import logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline Online Retail")
    parser.add_argument(
        "--env",
        default=None,
        help="Environnement de configuration (dev, prod). Défaut : ENV ou dev.",
    )
    parser.add_argument(
        "--analytics",
        action="store_true",
        help="Lance les analyses et le benchmark de partitionnement après l'écriture gold.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # 1. Configuration
    config = get_config(args.env)
    paths = config["paths"]
    logger.info("Environnement : %s", config["env"])
    logger.info("RAW  → %s", paths["raw"])
    logger.info("BRONZE → %s", paths["bronze"])
    logger.info("SILVER → %s", paths["silver"])
    logger.info("GOLD → %s", paths["gold"])

    spark = None
    try:
        # 2. SparkSession
        spark = create_spark_session(config["spark"])

        # 3. Lecture CSV brut
        df_raw = read_raw_csv(spark, paths["raw"])
        raw_count = df_raw.count()
        logger.info("Lignes brutes ingérées : %s", raw_count)

        # 4. Snapshot bronze
        write_delta(df_raw, paths["bronze"])
        logger.info("Bronze écrit : %s", paths["bronze"])

        # 5. Nettoyage → silver (en mémoire)
        df_silver = clean_transactions(df_raw)
        silver_count = df_silver.count()
        logger.info("Lignes après nettoyage : %s", silver_count)

        # 6. Contrôles qualité (lève DataQualityError si violation)
        quality_report = run_checks(df_silver, scope="cleaning", raise_on_failure=True)
        logger.info("Contrôles silver OK (%s lignes)", quality_report["row_count"])

        # 7. Persistance silver
        write_delta(df_silver, paths["silver"])
        logger.info("Silver écrit : %s", paths["silver"])

        # 8. Enrichissement → gold
        df_gold = enrich_transactions(df_silver)
        gold_report = run_checks(df_gold, scope="enriched", raise_on_failure=True)
        logger.info("Contrôles gold OK (%s lignes)", gold_report["row_count"])

        # 9. Persistance gold
        write_delta(df_gold, paths["gold"])
        logger.info("Gold écrit : %s", paths["gold"])

        # 10. Analytics optionnelles
        if args.analytics:
            run_analytics(spark, paths["gold"])

        logger.info("Pipeline terminé avec succès.")
        return 0

    except Exception:
        logger.exception("Échec du pipeline")
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
