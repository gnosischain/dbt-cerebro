

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  holders_in_bucket              AS value    
FROM `dbt`.`fct_execution_tokens_balance_cohorts_daily_agg`
WHERE date < today()
ORDER BY
  date,
  token,
  label