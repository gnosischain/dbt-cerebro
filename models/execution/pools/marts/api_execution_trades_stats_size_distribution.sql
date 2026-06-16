{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Trade-size distribution by time window. Each branch computes percentages over its
-- own window so the denominator is window-scoped. Dashboard filters by time_window.

SELECT '1m' AS time_window,
    size_bucket                                                                 AS label,
    round(100.0 * count() / (
        SELECT count() FROM {{ ref('int_execution_trades_by_tx') }}
        WHERE date >= today() - INTERVAL 30 DAY AND date < today()
          AND size_bucket != 'unknown'
    ), 2)                                                                       AS value,
    multiIf(
        size_bucket = '< $100',       1,
        size_bucket = '$100 – $1K',   2,
        size_bucket = '$1K – $10K',   3,
        size_bucket = '$10K – $100K', 4,
                                      5
    )                                                                           AS bucket_order
FROM {{ ref('int_execution_trades_by_tx') }}
WHERE date >= today() - INTERVAL 30 DAY AND date < today()
  AND size_bucket != 'unknown'
GROUP BY size_bucket

UNION ALL

SELECT '3m' AS time_window,
    size_bucket                                                                 AS label,
    round(100.0 * count() / (
        SELECT count() FROM {{ ref('int_execution_trades_by_tx') }}
        WHERE date >= today() - INTERVAL 90 DAY AND date < today()
          AND size_bucket != 'unknown'
    ), 2)                                                                       AS value,
    multiIf(
        size_bucket = '< $100',       1,
        size_bucket = '$100 – $1K',   2,
        size_bucket = '$1K – $10K',   3,
        size_bucket = '$10K – $100K', 4,
                                      5
    )                                                                           AS bucket_order
FROM {{ ref('int_execution_trades_by_tx') }}
WHERE date >= today() - INTERVAL 90 DAY AND date < today()
  AND size_bucket != 'unknown'
GROUP BY size_bucket

UNION ALL

SELECT '6m' AS time_window,
    size_bucket                                                                 AS label,
    round(100.0 * count() / (
        SELECT count() FROM {{ ref('int_execution_trades_by_tx') }}
        WHERE date >= today() - INTERVAL 180 DAY AND date < today()
          AND size_bucket != 'unknown'
    ), 2)                                                                       AS value,
    multiIf(
        size_bucket = '< $100',       1,
        size_bucket = '$100 – $1K',   2,
        size_bucket = '$1K – $10K',   3,
        size_bucket = '$10K – $100K', 4,
                                      5
    )                                                                           AS bucket_order
FROM {{ ref('int_execution_trades_by_tx') }}
WHERE date >= today() - INTERVAL 180 DAY AND date < today()
  AND size_bucket != 'unknown'
GROUP BY size_bucket

UNION ALL

SELECT '1y' AS time_window,
    size_bucket                                                                 AS label,
    round(100.0 * count() / (
        SELECT count() FROM {{ ref('int_execution_trades_by_tx') }}
        WHERE date >= today() - INTERVAL 365 DAY AND date < today()
          AND size_bucket != 'unknown'
    ), 2)                                                                       AS value,
    multiIf(
        size_bucket = '< $100',       1,
        size_bucket = '$100 – $1K',   2,
        size_bucket = '$1K – $10K',   3,
        size_bucket = '$10K – $100K', 4,
                                      5
    )                                                                           AS bucket_order
FROM {{ ref('int_execution_trades_by_tx') }}
WHERE date >= today() - INTERVAL 365 DAY AND date < today()
  AND size_bucket != 'unknown'
GROUP BY size_bucket
