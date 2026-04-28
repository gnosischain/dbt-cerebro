

SELECT
  address,
  project
FROM `dbt`.`int_crawlers_data_labels`
WHERE sector = 'DEX'