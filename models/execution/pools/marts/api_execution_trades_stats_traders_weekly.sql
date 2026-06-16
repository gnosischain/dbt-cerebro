{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'trades', 'api']
    )
}}

SELECT
    toMonday(date)          AS date,
    uniqExact(tx_from)      AS value
FROM {{ ref('int_execution_trades_by_tx') }}
WHERE date < today()
  AND tx_from IS NOT NULL
GROUP BY date
ORDER BY date
