

SELECT
  month AS date,
  ROUND(used * 100, 2) AS value
FROM `dbt`.`fct_execution_blocks_gas_usage_monthly`
ORDER BY date