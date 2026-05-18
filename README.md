# Pipeline PySpark — Online Retail

## Description

Ce projet met en place un pipeline de données sur le jeu public [Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) (UCI) : ingestion, nettoyage, enrichissement, contrôles qualité, stockage **Delta Lake** (bronze / silver / gold) et analyses (ventes, performance, historique Delta).

Deux modes d’exécution coexistent :

- **Notebook** `Online_retail_pipeline.ipynb` — exploration complète, contraintes Delta sur tables Spark, MERGE, time travel (environnement type Google Colab).
- **Pipeline Python** `src/main.py` — orchestration reproductible, configuration externalisée, tests unitaires.

Informations détaillées sur la base : `Online retail database informations.txt`.

## Architecture du projet

```
Spark on Online Retail/
├── config/
│   ├── dev.yaml              # chemins locaux + paramètres Spark
│   └── prod.yaml             # chemins cloud (S3, etc.)
├── data/
│   ├── raw/                  # CSV source
│   ├── bronze/               # snapshot brut (Delta)
│   ├── silver/               # données nettoyées (Delta)
│   └── gold/                 # données enrichies (Delta)
├── src/
│   ├── main.py               # point d’entrée du pipeline
│   ├── ingestion/
│   │   ├── spark_session.py  # Spark + Delta (Windows-friendly)
│   │   ├── read_data.py      # lecture CSV typée
│   │   └── write_data.py     # écriture Delta
│   ├── transformations/
│   │   ├── cleaning.py       # règles de nettoyage
│   │   └── enrichment.py     # segments, catégories, OrderAmount
│   ├── quality/
│   │   └── checks.py         # contrôles + rapport / exception
│   ├── analytics/
│   │   ├── sales_analysis.py
│   │   ├── performance_analysis.py
│   │   └── runner.py         # analyses post-pipeline
│   └── utils/
│       ├── config.py         # get_config()
│       └── logger.py
├── tests/
│   ├── test_cleaning.py
│   └── test_quality.py
├── Online_retail_pipeline.ipynb
├── requirements.txt
└── .env.example
```

## Le problème

Un e-commerce dispose d’environ **500k** lignes de transactions brutes avec valeurs manquantes, annulations, prix ou quantités aberrants et un schéma peu adapté aux KPI fiables. Objectifs :

- fiabiliser le chiffre d’affaires et les segments clients
- garantir la qualité des chargements (contrôles + contraintes Delta dans le notebook)
- accélérer les requêtes (partitionnement, format columnar)
- supporter l’évolution des données (MERGE, historique Delta).

## Pipeline industrialisé (`src/main.py`)

Flux exécuté dans l’ordre :

| Étape | Action |
|-------|--------|
| 1 | `get_config()` — charge `config/{env}.yaml` + surcharge `.env` |
| 2 | `create_spark_session()` — Spark local + extension Delta |
| 3 | `read_raw_csv()` → `df_raw` |
| 4 | `write_delta(df_raw, bronze)` — snapshot bronze |
| 5 | `clean_transactions(df_raw)` → `df_silver` |
| 6 | `run_checks(df_silver, scope="cleaning")` — lève `DataQualityError` si échec |
| 7 | `write_delta(df_silver, silver)` |
| 8 | `enrich_transactions(df_silver)` → `df_gold` |
| 9 | `run_checks(df_gold, scope="enriched")` puis `write_delta(df_gold, gold)` |
| 10 | `run_analytics()` si `--analytics` (CA par pays / catégorie + benchmark partitionnement) |

### Règles de nettoyage (`clean_transactions`)

- Suppression des nulls sur `CustomerID`, `UnitPrice`, `Quantity`, `InvoiceNo`
- Exclusion des annulations : `lower(InvoiceNo)` commence par `c`
- Exclusion de la facture aberrante `541431`
- Conservation des articles avec `length(StockCode) = 5`
- Cast : `Quantity` → int, `UnitPrice` → double, `InvoiceDate` → timestamp

### Enrichissement (`enrich_transactions`)

- `StockCode` → `ItemCode`, `OrderAmount` = `Quantity × UnitPrice`
- `Purchase_segment`, `Shopsize`, `Continent`, `product_category` (règles sur la description)

### Contrôles qualité (`run_checks`)

Retourne un rapport structuré (`passed`, `constraints`, `violations`) ou lève `DataQualityError`. Scopes : `cleaning` (silver) et `enriched` (gold).

## Prérequis

- **Python 3.10+** (testé avec 3.11)
- **JDK 11 ou 17** — `JAVA_HOME` doit pointer vers le dossier JDK **sans wildcard** (ex. éviter `jdk-17.*`)
- Fichier `data/raw/Online_Retail.csv` — [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)

Dépendances principales (`requirements.txt`) :

```
python-dotenv
pyspark==3.5.3
delta-spark==3.2.1
pyyaml
pytest
```

Pour le notebook : `ydata-profiling`, `pyngrok` (optionnel).

## Installation et lancement

```bash
# 1. Cloner le dépôt et installer les dépendances
pip install -r requirements.txt

# 2. Copier la configuration d’environnement (optionnel)
cp .env.example .env

# 3. Placer le CSV (ou adapter config/dev.yaml)
#    data/raw/Online_Retail.csv

# 4. Lancer le pipeline
python -m src.main

# Avec analyses et benchmark partitionné / non partitionné
python -m src.main --analytics

# Environnement prod (chemins S3 dans config/prod.yaml)
python -m src.main --env prod
```

Chemins par défaut (`config/dev.yaml`) :

| Couche | Chemin |
|--------|--------|
| raw | `data/raw/Online_Retail.csv` |
| bronze | `data/bronze` |
| silver | `data/silver` |
| gold | `data/gold` |

Surcharge via `.env` : `RAW_PATH`, `BRONZE_PATH`, `SILVER_PATH`, `GOLD_PATH`, `ENV`.

### Tests

```bash
python -m pytest tests/test_cleaning.py tests/test_quality.py -q
```

Le premier lancement peut être lent (démarrage JVM Spark).

### Notebook (Colab / Jupyter)

1. Ouvrir `Online_retail_pipeline.ipynb` et exécuter les cellules dans l’ordre.
2. Adapter le chemin CSV si besoin (historiquement `/Data_OR/Online_Retail.csv`).

> **Windows :** `spark_session.py` configure automatiquement `PYSPARK_PYTHON`, corrige un `JAVA_HOME` invalide et prépare `HADOOP_HOME` + `winutils.exe` si nécessaire.

## Présentation de la base de données

Transactions d’une boutique en ligne britannique (01/12/2010 – 09/12/2011). Une ligne = une ligne de facture (article × quantité × prix).

| Variable | Rôle | Commentaire |
|----------|------|-------------|
| `InvoiceNo` | Identifiant transaction | Préfixe **`C`** / **`c`** = annulation |
| `StockCode` | Référence produit | 5 caractères attendus en silver |
| `Description` | Libellé produit | Source des catégories enrichies |
| `Quantity` | Quantité | Cast entier après nettoyage |
| `InvoiceDate` | Horodatage | Format CSV `dd/MM/yyyy HH:mm:ss` |
| `UnitPrice` | Prix unitaire (GBP) | Cast double après nettoyage |
| `CustomerID` | Client | Nombreux nulls en brut |
| `Country` | Pays | Forte part UK |

**Schéma gold (enrichi).** `ItemCode`, `OrderAmount`, `Purchase_segment`, `Shopsize`, `Continent`, `product_category`, `desc_clean`.

## Réalisations

### Pipeline Python (`src/`)

| Composant | Contenu |
|-----------|---------|
| **Ingestion** | Lecture CSV typée, écriture Delta bronze / silver / gold |
| **Nettoyage** | Règles métier alignées sur le notebook (`phase1`) |
| **Qualité** | Contrôles automatisés avec rapport et arrêt en cas d’échec |
| **Enrichissement** | Colonnes analytiques pour la couche gold |
| **Analytics** | Agrégations CA ; benchmark partitionné vs non partitionné ; modules pour historique Delta (`performance_analysis.py`) |

### Notebook (`Online_retail_pipeline.ipynb`)

| Phase | Contenu |
|--------|---------|
| **Compréhension** | Schéma, stats, profils YData Profiling, explorations |
| **Nettoyage (`phase1`)** | Même logique métier que `cleaning.py` + table Delta |
| **Enrichissement (`phase2` → `phase3`)** | NLP léger sur descriptions, catégorisation avancée |
| **Organisation Delta** | Contraintes `CHECK` sur `phase3` / `phase4` |
| **Performance** | Table partitionnée `sales_per_country_continent`, plans `EXPLAIN`, mesures de temps |
| **Historique** | `MERGE INTO phase4`, `DESCRIBE HISTORY`, `VERSION AS OF` |

## Solution retenue

- **Medallion** bronze → silver → gold en Delta Lake (pipeline Python).
- **Règles métier** dans `cleaning.py`, vérifiées par `checks.py` avant publication silver/gold.
- **Delta Lake** dans le notebook pour ACID, contraintes, time travel et MERGE.
- **Partitionnement** (pays / continent) pour réduire le scan sur les agrégations par pays.

## Résultats obtenus (notebook — indicatifs)

- **Annulations** : ~8 839 lignes avec `InvoiceNo` commençant par `c` avant purge.
- **Performance** : CA par pays ~**2,45 s** (`phase4`) vs ~**0,90–1,55 s** (table partitionnée) selon le run.
- **MERGE** : ~355 281 lignes insérées dans l’expérience notebook.
- **Versions** : `phase1` v0 ≈ 8 247 147 £ vs `phase4` v1 ≈ 7 724 629 £ (périmètres différents après nettoyage / enrichissement).

Les chiffres varient selon la machine, le cache Spark et l’ordre d’exécution.

## Évolutions possibles

- Déploiement **Databricks** / cluster avec `config/prod.yaml` (S3 / ADLS).
- Compléter `tests/test_enrichment.py` et `great_expectations.py`.
- Scheduling (**Airflow**), OPTIMIZE / ZORDER Delta, monitoring qualité.
- MERGE et comparaisons d’historique intégrés au pipeline Python (au-delà du notebook).
- Analyses RFM / cohortes (`src/analytics/rfm.py`).
