

SELECT
  symbol      AS token,
  argMax(supply, date) AS value_native,
  argMax(supply_usd, date) AS value_usd
FROM `dbt`.`fct_execution_tokens_metrics_daily`
WHERE date < today()
GROUP BY symbol
ORDER BY token