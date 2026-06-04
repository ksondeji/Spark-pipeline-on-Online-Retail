"""Fonctions PySpark avec repli pour runtimes Databricks / PySpark < 3.5."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.functions import expr, when

_INT_PATTERN = r"^-?\d+$"
_DOUBLE_PATTERN = r"^-?\d+(\.\d+)?$"


def _column_name(column: Column) -> str | None:
    """Nom simple (ex. Quantity) si la colonne vient de col('Quantity')."""
    return getattr(column, "name", None) or getattr(column, "_name", None)


def _conditional_cast(column: Column, target_type: str) -> Column:
    """Cast uniquement si la valeur string respecte le motif (évite CAST_INVALID_INPUT)."""
    as_str = column.cast("string")
    t = target_type.strip().lower()
    if t in ("int", "integer"):
        return when(as_str.rlike(_INT_PATTERN), column.cast("int"))
    if t in ("double", "float"):
        return when(as_str.rlike(_DOUBLE_PATTERN), column.cast("double"))
    return column.cast(target_type)


def try_cast(column: Column, target_type: str) -> Column:
    """
    Cast tolérant : NULL si la conversion échoue.

    Ordre des replis :
    1. API Python ``try_cast`` (PySpark 3.5+)
    2. SQL ``try_cast`` via ``expr`` (Databricks Connect)
    3. ``when`` + regex (ANSI strict sans try_cast SQL)
    """
    try:
        from pyspark.sql.functions import try_cast as _try_cast

        return _try_cast(column, target_type)
    except ImportError:
        pass

    col_name = _column_name(column)
    if col_name:
        return expr(f"try_cast(`{col_name}` AS {target_type})")

    return _conditional_cast(column, target_type)
