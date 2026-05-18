from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Préfixes laissés tels quels (pas de résolution relative au repo)
REMOTE_PATH_PREFIXES = ("dbfs:", "abfss:", "s3://", "s3a://", "gs://", "/Volumes/")


def is_databricks_cluster() -> bool:
    """True si le code s'exécute sur un cluster ou runtime Serverless Databricks."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or os.environ.get("IS_SERVERLESS", "").upper() == "TRUE"
        or "DATABRICKS_SERVERLESS" in os.environ
    )


def is_databricks() -> bool:
    """Alias de is_databricks_cluster()."""
    return is_databricks_cluster()


def use_databricks_spark(config: dict[str, Any] | None = None) -> bool:
    """True si la session doit être créée via l'API Databricks (cluster ou Connect)."""
    if is_databricks_cluster():
        return True
    if config is None:
        return os.getenv("ENV", "").lower() == "databricks"
    return config.get("env") == "databricks" or config.get("runtime") == "databricks"


def _load_dotenv_if_local() -> None:
    if not is_databricks():
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")


def _resolve_path(value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith(REMOTE_PATH_PREFIXES):
        return value.rstrip("/")
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return str(path.resolve())


def _default_env_name() -> str:
    if is_databricks():
        return "databricks"
    return os.getenv("ENV", "dev")


def get_config(env: str | None = None) -> dict[str, Any]:
    """
    Charge config/{env}.yaml puis surcharge avec les variables d'environnement.

    Sur Databricks : ENV par défaut = ``databricks`` (fichier config/databricks.yaml).
    En local : ENV par défaut = ``dev`` + chargement du fichier ``.env``.
    """
    _load_dotenv_if_local()
    env_name = env or _default_env_name()
    config_file = PROJECT_ROOT / "config" / f"{env_name}.yaml"

    if not config_file.is_file():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {config_file}")

    with config_file.open(encoding="utf-8") as handle:
        file_cfg = yaml.safe_load(handle) or {}

    paths_cfg = file_cfg.get("paths") or {}
    if "raw" not in paths_cfg and file_cfg.get("raw_path"):
        paths_cfg["raw"] = file_cfg["raw_path"]
    if "gold" not in paths_cfg and file_cfg.get("curated_path"):
        paths_cfg["gold"] = file_cfg["curated_path"]

    paths = {
        "raw": _resolve_path(os.getenv("RAW_PATH") or paths_cfg.get("raw")),
        "bronze": _resolve_path(os.getenv("BRONZE_PATH") or paths_cfg.get("bronze")),
        "silver": _resolve_path(os.getenv("SILVER_PATH") or paths_cfg.get("silver")),
        "gold": _resolve_path(os.getenv("GOLD_PATH") or paths_cfg.get("gold")),
    }

    missing = [name for name, value in paths.items() if not value]
    if missing:
        raise ValueError(f"Chemins manquants dans la configuration : {', '.join(missing)}")

    spark_cfg = file_cfg.get("spark") or {}
    return {
        "env": env_name,
        "runtime": file_cfg.get("runtime", "databricks" if is_databricks() else "local"),
        "is_databricks": is_databricks_cluster(),
        "use_databricks_spark": use_databricks_spark(
            {"env": env_name, "runtime": file_cfg.get("runtime")}
        ),
        "project_root": str(PROJECT_ROOT),
        "paths": paths,
        "spark": {
            "app_name": spark_cfg.get("app_name", "OnlineRetail-pipeline"),
            "master": spark_cfg.get("master", "local[*]"),
            "driver_memory": spark_cfg.get("driver_memory", "4G"),
            "executor_memory": spark_cfg.get("executor_memory", "4G"),
        },
    }
