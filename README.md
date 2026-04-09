# Pipeline PySpark — Online Retail

## Description

Ce projet porte sur la mise en place d'un pipeline efficace de préparation des données couvrant les différents aspects suivants: ingestion du jeu de données public *Online Retail* (UCI), nettoyage, enrichissement, stockage sous **Delta Lake** avec contraintes, requêtes analytiques et optimisation de lecture via le partitionnement des données.

Le travail principal est documenté dans le notebook `Online_retail_pipeline.ipynb` (environnement type **Google Colab**). 
Des informations sur la base de données sont présentes dans le fichier `Online retail database informations.txt`.

## Le problème

Les données brutes e-commerce contiennent des valeurs manquantes, annulations, prix ou quantités aberrants et un schéma peu adapté aux analyses fiables. Le besoin est de construire un pipeline reproductible qui :

•	fiabilise les lignes utilisées pour le chiffre d’affaires et les segments clients ;
•	guarantisse la qualité des futurs chargements (contraintes sur les delta tables) ;
•	permet des plus requêtes rapides (optimisation grâce au format parquet) ;
•	supporte l’évolution des données (merge, historique Delta, comparaison de versions).

## Prérequis pour l'installation

- **Python 3** avec Jupyter ou Google Colab  
- Fichier `Online_Retail.csv` — [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)  
- Dépendances principales :  
  `pyspark==3.5.3`, `delta-spark==3.2.1`, `ydata-profiling`, `pyngrok` (optionnel)

## Installation et lancement

1. Cloner ou ouvrir ce dossier, placer `Online_Retail.csv` dans un répertoire accessible (dans le notebook Colab, le chemin utilisé est `/Data_OR/`).
2. Ouvrir `Online_retail_pipeline.ipynb` et exécuter les cellules **dans l’ordre**.
3. Adapter les chemins de lecture CSV si vous travaillez en local, par exemple remplacer `"/Data_OR/Online_Retail.csv"` par le chemin absolu de votre fichier.
4. La session Spark est créée en `local[*]` avec l’extension **Delta** (`DeltaSparkSessionExtension`, `DeltaCatalog`) et des paramètres mémoire adaptés au notebook (8G driver/executor dans la config d’origine).

> **Note :** Sur une machine locale, vérifiez la cohérence des versions **PySpark** et **delta-spark** ; ajustez la mémoire si besoin.

## Présentation de la base de données

Base de données [Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) : transactions d’une boutique en ligne britannique (cadeaux), sur la période 01/12/2010 – 09/12/2011. Granularité : une ligne = une ligne de facture (article × quantité × prix). Une part importante des clients sont des B2B (grossiste).

**Variables d’origine (fichier CSV)**

| Variable | Rôle | Type logique | Commentaire |
|----------|------|--------------|-------------|
| `InvoiceNo` | Identifiant de transaction | Chaîne / identifiant | Souvent numérique ; préfixe **`C`** = annulation. |
| `StockCode` | Référence produit | Chaîne (souvent 5 caractères) | Identifiant article. |
| `Description` | Libellé produit | Texte | Peut contenir des valeurs manquantes ou bruitées. |
| `Quantity` | Quantité vendue | Entier | Peut être négative (retours / annulations liées). |
| `InvoiceDate` | Horodatage | Date/heure | Format d’origine `dd/MM/yyyy HH:mm:ss` dans le CSV. |
| `UnitPrice` | Prix unitaire | Numérique (GBP) | Sterling par unité. |
| `CustomerID` | Client | Chaîne / identifiant | Nombreuses valeurs nulles en brut. |
| `Country` | Pays du client | Catégoriel | Forte concentration sur le **Royaume-Uni**. |

**Typage dans le pipeline.** Après ingestion PySpark, les types sont alignés explicitement : `Quantity` en entier, `UnitPrice` en double, `InvoiceDate` en **timestamp** ; les identifiants restent en chaîne pour préserver les codes non strictement numériques et le préfixe `C` des annulations.

**Observations utiles pour la préparation**

•	En terme de qualité : `CustomerID`, `Description`, parfois `Quantity` / `UnitPrice` absents ou incohérents ; lignes à exclure ou corriger pour des KPI de CA fiables.  
•	Les annulations : les factures dont `InvoiceNo` commence par `c` représentent des annulations et sont traitées à part dans le nettoyage.  
•	Ajout de colonnes : `OrderAmount` = `Quantity × UnitPrice` pour obtenir le montant de chaque commandes.

**Schéma enrichi (tables curées, ex. `phase3` / `phase4`).** S’ajoutent notamment : `OrderAmount`, `ItemCode` (renommage de `StockCode`, avec normalisation de longueur dans les contraintes), `Purchase_segment` (`High_spender`...), `Shopsize`, `Continent`, `product_category` (créé à partir de la description).

## Réalisations

| Phase | Contenu |
|--------|---------|
| **Compréhension** | Schéma, statistiques descriptives, profils **YData Profiling** sur des échantillons, exploration par pays / produits / montants. |
| **Nettoyage (`phase1`)** | Cast des types, parsing des dates, filtrage des nulls sur les champs clés, suppression des factures annulées (`InvoiceNo` commençant par `c`), filtrage des quantités et prix non pertinents, colonne **`OrderAmount`** = `Quantity × UnitPrice`, table Delta **`phase1`**. |
| **Enrichissement (`phase2` → `phase3`)** | Renommage `StockCode` → `ItemCode`, segments d’achat, **`Shopsize`**, **`Continent`**, catégorisation de produits à partir de la description (NLP léger : tokenisation, stop words, règles), table Delta **`phase3`**. |
| **Organisation Delta** | Contraintes **`CHECK`** sur `phase3` : `OrderAmount > 0`, pas d’annulation sur `InvoiceNo`, `length(ItemCode) = 5`. Export vers **`phase4`** (sans colonne intermédiaire `tokens`). |
| **Analyse & performance** | Agrégations (CA par pays, analyses par catégorie, etc.) ; table **`sales_per_country_continent`** partitionnée par **`Country`, `Continent`** ; mesure du temps d’exécution d’une requête de CA par pays **avec et sans** partitionnement ; table bucketisée **`sales_clus_country_Shopsize`** ; activation possible du **aggregate pushdown** Parquet. |
| **Nouvelles données & historique** | **`MERGE INTO phase4`** Utilisation de la base de données entières ; utilisation de **`DESCRIBE HISTORY`** et **`VERSION AS OF`** pour comparer l'agrégations entre versions. |

## Solution retenue

•	**Delta Lake** comme couche curated : ACID, schéma, contraintes et time travel.  
•	**Règles métier** codées en nettoyage SQL/Spark puis renforcées par contraintes pour les ingestions futures.  
•	**Partitionnement** aligné sur les filtres et groupements fréquents (pays / continent) pour réduire le scan des fichiers sur les requêtes de synthèse par pays.  
•	**Merge + historique** pour simuler l’arrivée de nouvelles lignes et auditer l’impact sur les KPI.

## Résultats obtenus (extraits du notebook)

•	**Annulations** : environ 8 839 lignes commençant par `c` avant purge.  
•	**Performance** : agrégation CA par pays — ~**2,45 s** sur `phase4` non partitionné vs ~1,55 s sur `sales_per_country_continent` partitionné (gain d’environ 0,90 s sur ce run).  
•	**Ingestion** : 355 281 lignes insérées via `MERGE` 
•	**Historique** : Utilisation de l'historique pour naviguer entre les versions.
- **Comparaison de versions** : exemple de revenus globaux — `phase1` VERSION 0 ≈ 8 247 147,31 vs `phase4` VERSION 1 ≈ 7 724 629,47 (écarts attendus car périmètre et transformations diffèrent entre `phase1` initial et le jeu curé/enrichi `phase4`).  

Les chiffres exacts peuvent varier selon la machine, l’ordre d’exécution et les caches Spark.

## Prochaines étapes possibles

- Déployer le pipeline sur **Databricks** ou **Spark cluster** avec chemins cloud (S3 / ADLS) et politiques de vacuum / retention Delta.  
- Remplacer les tables CSV partitionnées par des tables Delta partitionnées + `OPTIMIZE` / `ZORDER` sur les colonnes les plus filtrées.  
- Industrialiser : tests sur contraintes et volumétrie, scheduling (Airflow / dbt), et monitoring de la qualité (Great Expectations, Deequ).  
- Étendre les analyses (RFM, cohortes, panier moyen temporel) et documenter les SLA de fraîcheur des données.

