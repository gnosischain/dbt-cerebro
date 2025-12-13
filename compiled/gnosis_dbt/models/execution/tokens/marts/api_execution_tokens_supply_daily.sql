

SELECT
  date,
  symbol      AS token,
  token_class,
  supply      AS value
FROM `dbt`.`int_execution_tokens_value_daily`
WHERE date < today()
ORDER BY
  date,
  token