{{
    config(
        materialized='table',
        tags=['production', 'execution', 'pools', 'trades', 'fct']
    )
}}

WITH

sides AS (
    SELECT
        toDate(block_timestamp)             AS date,
        token_sold_symbol                   AS token,
        'sold'                              AS side,
        coalesce(amount_usd, 0)             AS usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE token_sold_symbol != ''

    UNION ALL

    SELECT
        toDate(block_timestamp)             AS date,
        token_bought_symbol                 AS token,
        'bought'                            AS side,
        coalesce(amount_usd, 0)             AS usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE token_bought_symbol != ''
)

SELECT
    date,
    token,
    round(sumIf(usd, side = 'bought'), 2)               AS bought_usd,
    round(sumIf(usd, side = 'sold'),   2)               AS sold_usd,
    countIf(side = 'bought')                            AS bought_trades,
    countIf(side = 'sold')                              AS sold_trades,
    round(sum(usd), 2)                                  AS combined_usd
FROM sides
GROUP BY date, token
