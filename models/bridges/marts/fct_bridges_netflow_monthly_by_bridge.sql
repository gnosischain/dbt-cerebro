{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(month, bridge)',
  unique_key='(month, bridge)',
  partition_by='month',
  tags=['production','intermediate','bridges']
) }}

WITH m AS (
  SELECT
    toStartOfMonth(date) AS month,
    bridge,
    sumIf(volume_usd, direction='in')  AS inflow_usd,
    sumIf(volume_usd, direction='out') AS outflow_usd
  FROM {{ ref('int_bridges_flows_daily') }}
  WHERE date < toStartOfMonth(today())
  {{ apply_monthly_incremental_filter('date', 'month', 'true') }}
  GROUP BY month, bridge
),
n AS (
  SELECT
    month,
    bridge,
    inflow_usd - outflow_usd AS netflow_usd_month
  FROM m
)
SELECT
  n.month,
  n.bridge,
  n.netflow_usd_month,
  sum(n.netflow_usd_month) OVER (
    PARTITION BY n.bridge ORDER BY n.month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_netflow_usd
FROM n