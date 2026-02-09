

SELECT
  date,
  symbol      AS token,
  token_class,
  holders     AS value
FROM `dbt`.`fct_execution_tokens_metrics_daily`
WHERE date < today()
ORDER BY
  date,
  token