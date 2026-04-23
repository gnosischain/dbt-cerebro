{{
    config(
        materialized='table',
        tags=['dev', 'execution', 'pools', 'trades', 'fct']
    )
}}

SELECT
    toDate(block_timestamp)             AS date,
    protocol,
    count()                             AS swap_count,
    round(sum(amount_usd), 2)           AS volume_usd
FROM {{ ref('int_execution_pools_dex_trades') }}
WHERE protocol != ''
GROUP BY date, protocol
