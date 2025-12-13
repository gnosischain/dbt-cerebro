

SELECT
  symbol      AS token,
  argMax(supply, date) AS value
FROM `dbt`.`int_execution_tokens_value_daily`
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token