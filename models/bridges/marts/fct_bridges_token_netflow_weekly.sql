{{ config(
  materialized='table',
  engine='MergeTree()',
  order_by='(week, token)',
  partition_by='toStartOfMonth(week)',
  tags=['production','intermediate','bridges']
) }}

SELECT
  toStartOfWeek(date, 1) AS week,
  token,
  sum(net_usd)           AS netflow_usd_week
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < toStartOfWeek(today(), 1)
GROUP BY week, token