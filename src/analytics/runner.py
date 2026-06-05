from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from src.analytics.delta_tables import (
    full_table_name,
    register_notebook_tables,
)
from src.analytics.performance_analysis import (
    benchmark_partitioned_vs_non_partitioned,
    compare_revenue_by_country_versions,
    compare_total_revenue_versions,
    get_table_history,
    summarize_revenue_deltas,
)
from src.analytics.sales_analysis import (
    get_cumulative_contribution_by_country,
    get_mean_spend_by_segment,
    get_sales_by_continent,
    get_sales_by_country,
    get_sales_by_product_category,
    get_sales_by_purchase_segment,
    get_sales_by_shopsize,
    get_top_category_for_big_customer,
)
from src.utils.logger import logger


def _resolve_time_travel_versions(
    spark: SparkSession,
    table_v0_fqn: str,
    table_v1_fqn: str,
) -> tuple[int, int]:
    """Choisit les versions 0 et 1 si disponibles (sinon la dernière)."""
    hist_v0 = spark.sql(f"DESCRIBE HISTORY {table_v0_fqn}")
    hist_v1 = spark.sql(f"DESCRIBE HISTORY {table_v1_fqn}")
    max_v0 = hist_v0.agg({"version": "max"}).collect()[0][0]
    max_v1 = hist_v1.agg({"version": "max"}).collect()[0][0]
    version_v0 = 0 if max_v0 is not None and max_v0 >= 0 else 0
    version_v1 = 1 if max_v1 is not None and max_v1 >= 1 else int(max_v1 or 0)
    return int(version_v0), int(version_v1)


def _show_section(title: str, df, limit: int | None = 20) -> None:
    logger.info("=== %s ===", title)
    if limit is None:
        df.show(truncate=False)
    else:
        df.show(limit, truncate=False)


def run_analytics(spark: SparkSession, gold_path: str) -> None:
    """Analyses ventes simples (sans tables UC ni time travel)."""
    logger.info("=== Analytique : CA par pays ===")
    get_sales_by_country(spark, gold_path).show(20, truncate=False)

    logger.info("=== Analytique : CA par catégorie produit ===")
    get_sales_by_product_category(spark, gold_path).show(20, truncate=False)

    logger.info("=== Analytique : CA par continent ===")
    get_sales_by_continent(spark, gold_path).show(truncate=False)

    logger.info("=== Analytique : CA par segment d'achat ===")
    get_sales_by_purchase_segment(spark, gold_path).show(truncate=False)


def run_full_analytics_report(
    spark: SparkSession,
    config: dict[str, Any],
    *,
    simulate_phase4_merge: bool = True,
) -> dict[str, Any]:
    """
    Rapport complet aligné notebook : tables phase*, partitionnement,
    DESCRIBE HISTORY, time travel phase1 v0 vs phase4 v1.
    """
    paths = config["paths"]
    tables_cfg = config.get("tables") or {}
    catalog = tables_cfg.get("catalog", "main")
    schema = tables_cfg.get("schema", "default")

    logger.info("--- Enregistrement des tables notebook (phase1…phase4) ---")
    registered = register_notebook_tables(
        spark,
        paths,
        tables_cfg,
        simulate_phase4_merge=simulate_phase4_merge,
    )

    phase1_fqn = registered["phase1"]
    phase4_fqn = registered["phase4"]
    partitioned_fqn = registered["sales_per_country_continent"]

    gold_path = paths["gold"]

    logger.info("--- Analyses ventes (gold) ---")
    _show_section("CA par pays", get_sales_by_country(spark, gold_path).orderBy("total_revenue", ascending=False))
    _show_section("CA par catégorie", get_sales_by_product_category(spark, gold_path))
    _show_section("CA par continent", get_sales_by_continent(spark, gold_path))
    _show_section("CA par segment", get_sales_by_purchase_segment(spark, gold_path))
    _show_section("CA par taille boutique", get_sales_by_shopsize(spark, gold_path), limit=None)
    _show_section("Panier moyen par segment", get_mean_spend_by_segment(spark, gold_path), limit=None)
    _show_section(
        "Top catégories (High_spender)",
        get_top_category_for_big_customer(spark, gold_path),
    )
    _show_section(
        "Contribution cumulative par pays",
        get_cumulative_contribution_by_country(spark, gold_path),
    )

    logger.info("--- Benchmark partitionnement (phase4 vs sales_per_country_continent) ---")
    bench = benchmark_partitioned_vs_non_partitioned(
        spark,
        non_partitioned_table=phase4_fqn,
        partitioned_table=partitioned_fqn,
    )
    logger.info(
        "Non partitionné (%s) : %.2fs | Partitionné (%s) : %.2fs | gain : %s%%",
        phase4_fqn,
        bench["non_partitioned"]["elapsed_seconds"],
        partitioned_fqn,
        bench["partitioned"]["elapsed_seconds"],
        f"{bench['speedup_pct']:.1f}" if bench["speedup_pct"] is not None else "n/a",
    )

    logger.info("--- DESCRIBE HISTORY phase4 ---")
    history_df = get_table_history(spark, phase4_fqn)
    history_df.show(truncate=False)

    version_v0, version_v1 = _resolve_time_travel_versions(
        spark, phase1_fqn, phase4_fqn
    )
    logger.info(
        "Time travel : %s VERSION %s vs %s VERSION %s",
        phase1_fqn,
        version_v0,
        phase4_fqn,
        version_v1,
    )

    revenue_delta = compare_revenue_by_country_versions(
        spark,
        table_v0=phase1_fqn,
        version_v0=version_v0,
        table_v1=phase4_fqn,
        version_v1=version_v1,
        label_v0="v0",
        label_v1="v1",
    )
    _show_section("Écart CA par pays (time travel)", revenue_delta)

    delta_summary = summarize_revenue_deltas(
        spark,
        table_v0=phase1_fqn,
        version_v0=version_v0,
        table_v1=phase4_fqn,
        version_v1=version_v1,
        label_v0="v0",
        label_v1="v1",
    )
    _show_section("Synthèse deltas positifs / négatifs", delta_summary, limit=None)

    totals = compare_total_revenue_versions(
        spark,
        [
            (phase1_fqn, version_v0, f"phase1_v{version_v0}"),
            (phase4_fqn, version_v1, f"phase4_v{version_v1}"),
        ],
    )
    logger.info("CA global par version : %s", totals)

    report: dict[str, Any] = {
        "registered_tables": registered,
        "partition_benchmark": bench,
        "history": history_df,
        "revenue_by_country_delta": revenue_delta,
        "revenue_delta_summary": delta_summary,
        "total_revenue_by_version": totals,
        "time_travel_versions": {"phase1": version_v0, "phase4": version_v1},
    }

    logger.info(
        "Tables SQL disponibles : %s",
        ", ".join(
            full_table_name(catalog, schema, name)
            for name in (
                "phase1",
                "phase2",
                "phase3",
                "phase4",
                "df_rest_phase5",
                "sales_per_country_continent",
                "sales_clus_country_Shopsize",
                "product_category_per_continent",
            )
        ),
    )
    return report
