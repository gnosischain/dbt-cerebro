{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(month, bridge)',
  unique_key='(month, bridge)',
  partition_by='month',
  tags=['production','intermediate','bridges']
) }}

SELECT
  toStartOfMonth(date) AS month,
  bridge,
  sumIf(volume_usd, direction='in')  AS inflow_usd_month,
  sumIf(volume_usd, direction='out') AS outflow_usd_month
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < toStartOfMonth(today())
{{ apply_monthly_incremental_filter('date', 'month', 'true') }}
GROUP BY month, bridge