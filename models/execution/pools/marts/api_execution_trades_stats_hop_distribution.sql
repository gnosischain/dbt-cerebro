{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Static hop-count distribution over the last 30 days. One row per bucket
-- (1, 2, 3, 4+ hops) with the share (%) of trades and raw count. Not
-- affected by the dashboard's time window — the chart label notes the
-- fixed 30d window.

WITH

trades AS (
    SELECT
        transaction_hash,
        count()                                             AS hop_count
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE block_timestamp >= today() - INTERVAL 30 DAY
      AND block_timestamp < today()
    GROUP BY transaction_hash
),

bucketed AS (
    SELECT
        multiIf(
            hop_count = 1, '1 hop',
            hop_count = 2, '2 hops',
            hop_count = 3, '3 hops',
            '4+ hops'
        )                                                   AS bucket,
        multiIf(
            hop_count = 1, 1,
            hop_count = 2, 2,
            hop_count = 3, 3,
            4
        )                                                   AS bucket_order
    FROM trades
),

totals AS (
    SELECT count() AS total_trades FROM bucketed
)

SELECT
    b.bucket                                                AS label,
    round(100.0 * count() / any(t.total_trades), 2)         AS value,
    count()                                                 AS trade_count,
    min(b.bucket_order)                                     AS bucket_order
FROM bucketed b
CROSS JOIN totals t
GROUP BY b.bucket
ORDER BY bucket_order
