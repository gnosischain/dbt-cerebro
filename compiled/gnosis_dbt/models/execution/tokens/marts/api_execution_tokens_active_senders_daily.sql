

SELECT
  date,
  symbol          AS token,
  token_class,
  active_senders  AS value
FROM `dbt`.`fct_execution_tokens_metrics_daily`
WHERE date < today()
ORDER BY
  date,
  token