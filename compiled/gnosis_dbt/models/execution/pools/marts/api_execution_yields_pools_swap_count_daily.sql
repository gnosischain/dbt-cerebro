

SELECT
    date,
    token,
    pool AS label,
    swap_count AS value
FROM `dbt`.`fct_execution_yields_pools_daily`
WHERE swap_count IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label