

SELECT
    date,
    token,
    pool AS label,
    fees_usd_daily AS value
FROM `dbt`.`fct_execution_pools_daily`
WHERE fees_usd_daily IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label