{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(action, week, token)',
    tags=['production','execution','gpay']
  )
}}


SELECT
    action,
    week,
    token,
    volume,
    volume_usd,
    activity_count,
    SUM(volume) OVER (PARTITION BY action, token ORDER BY week) AS volume_cumulative,
    SUM(volume_usd) OVER (PARTITION BY action, token ORDER BY week) AS volume_usd_cumulative,
    SUM(activity_count) OVER (PARTITION BY action, token ORDER BY week) AS activity_count_cumulative
FROM (
  SELECT
      action,
      toStartOfWeek(date, 1) AS week,
      symbol                 AS token,
      sum(amount)            AS volume,
      sum(amount_usd)        AS volume_usd,
      sum(activity_count)    AS activity_count
  FROM {{ ref('int_execution_gpay_activity_daily') }}
  WHERE toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
  GROUP BY action, week, token
)
ORDER BY action, week, token
