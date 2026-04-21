{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Static trade-size distribution over the last 30 days. Trade USD is the
-- max of per-hop amount_usd within a transaction (matches the
-- `api_execution_live_trades.trade_usd` convention). Not affected by the
-- dashboard's time window — the chart label notes the fixed 30d window.

WITH

trades AS (
    SELECT
        transaction_hash,
        max(amount_usd)                                     AS trade_usd
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE block_timestamp >= today() - INTERVAL 30 DAY
      AND block_timestamp < today()
      AND amount_usd IS NOT NULL
    GROUP BY transaction_hash
),

bucketed AS (
    SELECT
        multiIf(
            trade_usd < 100,      '< $100',
            trade_usd < 1000,     '$100 – $1K',
            trade_usd < 10000,    '$1K – $10K',
            trade_usd < 100000,   '$10K – $100K',
                                  '$100K+'
        )                                                   AS bucket,
        multiIf(
            trade_usd < 100,      1,
            trade_usd < 1000,     2,
            trade_usd < 10000,    3,
            trade_usd < 100000,   4,
                                  5
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
