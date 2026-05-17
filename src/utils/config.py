from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_path(value: str | None) -> str | None:
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return str(path.resolve())


def get_config(env: str | None = None) -> dict[str, Any]:
    """
    Charge config/{env}.yaml puis surcharge avec les variables d'environnement.
  Priorité : variables d'env > fichier YAML > valeurs par défaut.
    """
    load_dotenv(PROJECT_ROOT / ".env")
    env_name = env or os.getenv("ENV", "dev")
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
        "project_root": str(PROJECT_ROOT),
        "paths": paths,
        "spark": {
            "app_name": spark_cfg.get("app_name", "OnlineRetail-pipeline"),
            "master": spark_cfg.get("master", "local[*]"),
            "driver_memory": spark_cfg.get("driver_memory", "4G"),
            "executor_memory": spark_cfg.get("executor_memory", "4G"),
        },
    }
