SELECT
    date
    ,client
    ,pct AS value
FROM {{ ref('fct_execution_blocks_clients_daily') }}
ORDER BY date, client