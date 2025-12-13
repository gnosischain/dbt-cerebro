

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  value_usd_in_bucket            AS value    
FROM `dbt`.`fct_execution_tokens_balance_cohorts_daily_agg`
WHERE date < today()
ORDER BY
  date,
  token,
  label