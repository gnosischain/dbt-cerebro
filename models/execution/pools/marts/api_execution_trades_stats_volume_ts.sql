{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

SELECT
    date,
    protocol            AS label,
    volume_usd          AS value
FROM {{ ref('fct_execution_trades_by_protocol_daily') }}
WHERE date < today()
ORDER BY date, label
