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

from src.utils.config import is_databricks_cluster, use_databricks_spark

WINUTILS_URL = (
    "https://raw.githubusercontent.com/cdarlint/winutils/master/"
    "hadoop-3.3.5/bin/winutils.exe"
)

_CLUSTER_ID_ENV_KEYS = (
    "DATABRICKS_CLUSTER_ID",
    "CLUSTER_ID",
    "DB_CLUSTER_ID",
)


def get_spark(config: dict[str, Any] | None = None) -> SparkSession:
    if use_databricks_spark(config):
        return _create_databricks_session(config)
    return create_spark_session((config or {}).get("spark"))


def _get_notebook_spark() -> SparkSession | None:
    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip is not None:
            notebook_spark = ip.user_ns.get("spark")
            if notebook_spark is not None:
                return notebook_spark
    except Exception:
        pass
    return None


def _resolve_cluster_id(config: dict[str, Any] | None) -> str | None:
    spark_cfg = (config or {}).get("spark", {})
    cluster_id = spark_cfg.get("cluster_id") or os.getenv("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        return str(cluster_id).strip()

    for key in _CLUSTER_ID_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value.strip()

    for path in (
        Path("/databricks/driver/conf/cluster-id"),
        Path("/databricks/spark/conf/cluster-id"),
    ):
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()

    return None


def _use_serverless(config: dict[str, Any] | None) -> bool:
    spark_cfg = (config or {}).get("spark", {})
    if spark_cfg.get("serverless") is True:
        return True
    return os.environ.get("IS_SERVERLESS", "").upper() == "TRUE"


def _create_databricks_session(config: dict[str, Any] | None = None) -> SparkSession:
    """
    Session Databricks :
    1. session active ou variable notebook ``spark``
    2. DatabricksSession avec cluster_id ou serverless (terminal / job)
    """
    spark = SparkSession.getActiveSession() or _get_notebook_spark()
    if spark is not None:
        return spark

    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "Package 'databricks-connect' requis. "
            "Installez-le sur le cluster : %pip install databricks-connect"
        ) from exc

    builder = DatabricksSession.builder

    if _use_serverless(config):
        return builder.serverless().getOrCreate()

    cluster_id = _resolve_cluster_id(config)
    if cluster_id:
        return builder.clusterId(cluster_id).getOrCreate()

    if is_databricks_cluster():
        raise RuntimeError(
            "Impossible de créer une session Spark sur Databricks : "
            "aucun cluster_id détecté.\n"
            "Solutions :\n"
            "  1. Exécuter depuis un notebook (la variable spark existe déjà), ou\n"
            "  2. Ajouter dans config/databricks.yaml :\n"
            "       spark:\n"
            "         cluster_id: \"<id-du-cluster>\"\n"
            "     (Compute → votre cluster → URL contient cluster/XXXXXXXX),\n"
            "  3. Ou pour Serverless :\n"
            "       spark:\n"
            "         serverless: true\n"
            "  4. Ou exporter DATABRICKS_CLUSTER_ID avant python -m src.main"
        )

    return builder.getOrCreate()


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
