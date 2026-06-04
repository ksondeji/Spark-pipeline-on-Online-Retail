"""Fonctions PySpark avec repli pour runtimes Databricks / PySpark < 3.5."""

from __future__ import annotations

from pyspark.sql import Column


def try_cast(column: Column, target_type: str) -> Column:
    """
    Cast tolérant : null si la conversion échoue (PySpark 3.5+).

    Sur Databricks étudiant (PySpark 3.12 packagé sans try_cast Python),
    repli sur ``cast`` — à utiliser après filtres regex / lecture Delta.
    """
    try:
        from pyspark.sql.functions import try_cast as _try_cast

        return _try_cast(column, target_type)
    except ImportError:
        return column.cast(target_type)
