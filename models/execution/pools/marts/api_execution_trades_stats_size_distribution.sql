{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Trade-size distribution over the last 30 days. Light GROUP BY on the
-- pre-bucketed size_bucket column in int_execution_trades_by_tx.

WITH

recent AS (
    SELECT size_bucket
    FROM {{ ref('int_execution_trades_by_tx') }}
    WHERE date >= today() - INTERVAL 30 DAY
      AND date <  today()
      AND size_bucket != 'unknown'
),

total AS (
    SELECT count() AS n FROM recent
)

SELECT
    size_bucket                                                                 AS label,
    count()                                                                     AS trade_count,
    round(100.0 * count() / (SELECT n FROM total), 2)                           AS value,
    multiIf(
        size_bucket = '< $100',       1,
        size_bucket = '$100 – $1K',   2,
        size_bucket = '$1K – $10K',   3,
        size_bucket = '$10K – $100K', 4,
                                      5
    )                                                                           AS bucket_order
FROM recent
GROUP BY size_bucket
ORDER BY bucket_order
