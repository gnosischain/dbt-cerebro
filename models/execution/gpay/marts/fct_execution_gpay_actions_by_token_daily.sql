{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(action, date, token)',
    tags=['production','execution','gpay']
  )
}}


SELECT
    action,
    date,
    token,
    volume,
    volume_usd,
    activity_count,
    SUM(volume) OVER (PARTITION BY action, token ORDER BY date) AS volume_cumulative,
    SUM(volume_usd) OVER (PARTITION BY action, token ORDER BY date) AS volume_usd_cumulative,
    SUM(activity_count) OVER (PARTITION BY action, token ORDER BY date) AS activity_count_cumulative
FROM (
    SELECT
        action,
        date,
        symbol                 AS token,
        sum(amount)            AS volume,
        sum(amount_usd)        AS volume_usd,
        sum(activity_count)    AS activity_count
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE date < today()
    GROUP BY action, date, token
)
ORDER BY action, date, token
