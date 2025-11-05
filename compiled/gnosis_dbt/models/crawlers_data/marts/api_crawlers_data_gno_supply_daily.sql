

SELECT
  label,
  block_date AS date,
  supply    
FROM `dbt`.`stg_crawlers_data__dune_gno_supply`
ORDER BY date, label