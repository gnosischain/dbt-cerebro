

SELECT
  symbol      AS token,
  toUInt64(argMax(holders, date)) AS value
FROM `dbt`.`int_execution_tokens_value_daily`
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token