{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(month, token)',
  unique_key='(month, token)',
  partition_by='month',
  tags=['production','intermediate','bridges']
) }}

SELECT
  toStartOfMonth(date) AS month,
  token,
  sumIf(volume_usd, direction='in')  - sumIf(volume_usd, direction='out') AS netflow_usd_month
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < today()
{{ apply_monthly_incremental_filter('date', 'month', 'true') }}
GROUP BY month, token