

SELECT DISTINCT
  project,
  sector
FROM `dbt`.`int_crawlers_data_labels`
WHERE project IS NOT NULL
  AND sector  IS NOT NULL