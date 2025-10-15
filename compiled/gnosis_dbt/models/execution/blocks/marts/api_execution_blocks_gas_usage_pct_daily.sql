

SELECT
  date,
  ROUND(gas_used_fraq * 100, 2) AS value
FROM `dbt`.`int_execution_blocks_gas_usage_daily`
WHERE date < today()   
ORDER BY date