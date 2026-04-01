

SELECT
    date,
    token,
    pool AS label,
    'Volume (USD)' AS volume_type,
    volume_usd_daily AS value
FROM `dbt`.`fct_execution_pools_daily`
WHERE volume_usd_daily IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label