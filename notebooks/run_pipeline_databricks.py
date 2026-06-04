# Databricks notebook source
# MAGIC %md
# MAGIC # Lancer le pipeline (compte étudiant / Serverless)
# MAGIC
# MAGIC 1. Attache ce repo au notebook (ou ouvre ce fichier dans le workspace).
# MAGIC 2. **Synchronise le code** : Repos → *Spark-pipeline-on-Online-Retail* → **Pull** (ou `git pull` si le repo est à jour sur GitHub).
# MAGIC 3. Exécute la cellule ci-dessous.

# COMMAND ----------

# MAGIC %pip install -q -r requirements-databricks.txt

# COMMAND ----------

import importlib
import sys

# Évite que argparse lise les arguments du kernel notebook
sys.argv = ["run_pipeline"]

import src.main as pipeline_module

importlib.reload(pipeline_module)

if hasattr(pipeline_module, "run_pipeline"):
    exit_code = pipeline_module.run_pipeline()
else:
    # Ancienne version de main.py sur le workspace (sans run_pipeline)
    exit_code = pipeline_module.main()

if exit_code != 0:
    raise RuntimeError(f"Pipeline terminé avec le code {exit_code}")
