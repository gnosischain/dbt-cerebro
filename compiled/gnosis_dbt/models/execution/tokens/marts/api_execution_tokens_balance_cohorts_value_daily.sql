

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  value_native_in_bucket         AS value_native,
  value_usd_in_bucket            AS value_usd
FROM `dbt`.`int_execution_tokens_balance_cohorts_daily`
WHERE date < today()
ORDER BY
  date,
  token,
  label