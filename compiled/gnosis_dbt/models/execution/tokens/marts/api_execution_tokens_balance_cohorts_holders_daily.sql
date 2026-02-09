

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  holders_in_bucket              AS value    
FROM `dbt`.`int_execution_tokens_balance_cohorts_daily`
WHERE date < today()
ORDER BY
  date,
  token,
  label