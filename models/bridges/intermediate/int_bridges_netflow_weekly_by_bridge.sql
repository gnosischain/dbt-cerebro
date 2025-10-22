{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(week, bridge)',
  unique_key='(week, bridge)',
  partition_by='toStartOfMonth(week)',
  tags=['production','intermediate','bridges']
) }}

WITH w AS (
  SELECT
    toStartOfWeek(date) AS week,
    bridge,
    sumIf(volume_usd, direction='in')  AS inflow_usd,
    sumIf(volume_usd, direction='out') AS outflow_usd
  FROM {{ ref('int_bridges_flows_daily') }}
  WHERE date < today()
  {{ apply_monthly_incremental_filter('date', 'week', 'true') }}
  GROUP BY week, bridge
),
n AS (
  SELECT
    week,
    bridge,
    inflow_usd - outflow_usd AS netflow_usd_week
  FROM w
)
SELECT
  n.week,
  n.bridge,
  n.netflow_usd_week,
  sum(n.netflow_usd_week) OVER (PARTITION BY n.bridge ORDER BY n.week
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_netflow_usd
FROM n