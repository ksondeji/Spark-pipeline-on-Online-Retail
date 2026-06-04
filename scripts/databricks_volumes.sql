-- Option alternative : un volume Unity Catalog par couche (medallion).
-- À exécuter dans un notebook SQL Databricks si vous préférez des volumes séparés
-- plutôt que des sous-dossiers dans le volume "raw".
--
-- Puis dans config/databricks.yaml :
--   bronze: "/Volumes/main/default/bronze"
--   silver: "/Volumes/main/default/silver"
--   gold:   "/Volumes/main/default/gold"

CREATE VOLUME IF NOT EXISTS main.default.bronze;
CREATE VOLUME IF NOT EXISTS main.default.silver;
CREATE VOLUME IF NOT EXISTS main.default.gold;

SHOW VOLUMES IN main.default;
