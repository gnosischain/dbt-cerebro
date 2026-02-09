

SELECT
  date,
  symbol      AS token,
  token_class,
  supply      AS value_native,
  supply_usd  AS value_usd
FROM `dbt`.`fct_execution_tokens_metrics_daily`
WHERE date < today()
ORDER BY
  date,
  token