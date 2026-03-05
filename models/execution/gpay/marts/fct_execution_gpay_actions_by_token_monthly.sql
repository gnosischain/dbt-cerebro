{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(action, month, token)',
    tags=['production','execution','gpay']
  )
}}


SELECT
    action,
    month,
    token,
    volume,
    volume_usd,
    activity_count,
    SUM(volume) OVER (PARTITION BY action, token ORDER BY month) AS volume_cumulative,
    SUM(volume_usd) OVER (PARTITION BY action, token ORDER BY month) AS volume_usd_cumulative,
    SUM(activity_count) OVER (PARTITION BY action, token ORDER BY month) AS activity_count_cumulative
FROM (
  SELECT
      action,
      toStartOfMonth(date)   AS month,
      symbol                 AS token,
      sum(amount)            AS volume,
      sum(amount_usd)        AS volume_usd,
      sum(activity_count)    AS activity_count
  FROM {{ ref('int_execution_gpay_activity_daily') }}
  WHERE toStartOfMonth(date) < toStartOfMonth(today())
  GROUP BY action, month, token
)
ORDER BY action, month, token
