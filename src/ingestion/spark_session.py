from __future__ import annotations

import os
import platform
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import Any

from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.utils.config import is_databricks

WINUTILS_URL = (
    "https://raw.githubusercontent.com/cdarlint/winutils/master/"
    "hadoop-3.3.5/bin/winutils.exe"
)


def get_spark(config: dict[str, Any] | None = None) -> SparkSession:
    """
    Point d'entrée unique : session Databricks (Delta natif) ou session locale.
    """
    if is_databricks():
        return _create_databricks_session(config)
    return create_spark_session((config or {}).get("spark"))


def _create_databricks_session(config: dict[str, Any] | None = None) -> SparkSession:
    """
    Sur Databricks, Spark et Delta sont déjà configurés par le runtime.
    On réutilise la session active ou on en crée une via le builder du cluster.
    """
    spark_config = (config or {}).get("spark", {})
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = (
            SparkSession.builder.appName(
                spark_config.get("app_name", "OnlineRetail-pipeline")
            )
            .getOrCreate()
        )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _fix_java_home() -> None:
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home and "*" not in java_home and Path(java_home).is_dir():
        return

    java_exe = shutil.which("java")
    if not java_exe:
        raise RuntimeError(
            "Java introuvable. Installez un JDK 11/17 et définissez JAVA_HOME "
            "sur le dossier d'installation (sans caractère '*')."
        )

    os.environ["JAVA_HOME"] = str(Path(java_exe).resolve().parent.parent)


def _setup_hadoop_home_windows() -> str | None:
    if platform.system() != "Windows":
        return None

    hadoop_home = Path(os.environ.get("LOCALAPPDATA", "C:/temp")) / "spark-hadoop"
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    winutils = bin_dir / "winutils.exe"

    if not winutils.exists():
        urllib.request.urlretrieve(WINUTILS_URL, winutils)

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    return str(hadoop_home)


def _configure_pyspark_env() -> str | None:
    _fix_java_home()

    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    import pyspark

    os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).resolve().parent)

    spark_tmp = Path(os.environ.get("LOCALAPPDATA", "C:/temp")) / "spark-tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("SPARK_LOCAL_DIRS", str(spark_tmp))
    return _setup_hadoop_home_windows()


def create_spark_session(spark_config: dict[str, Any] | None = None) -> SparkSession:
    """Crée une SparkSession locale avec l'extension Delta Lake (hors Databricks)."""
    hadoop_home = _configure_pyspark_env()

    spark_config = spark_config or {}
    spark_tmp = Path(os.environ["SPARK_LOCAL_DIRS"])

    conf = (
        SparkConf()
        .setAppName(spark_config.get("app_name", "OnlineRetail-pipeline"))
        .setMaster(spark_config.get("master", "local[*]"))
        .set("spark.driver.memory", spark_config.get("driver_memory", "4G"))
        .set("spark.executor.memory", spark_config.get("executor_memory", "4G"))
        .set("spark.local.dir", str(spark_tmp))
    )
    if hadoop_home:
        conf = conf.set("spark.hadoop.hadoop.home.dir", hadoop_home)

    conf = (
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
