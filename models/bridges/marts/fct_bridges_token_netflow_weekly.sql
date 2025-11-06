{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(week, token)',
  unique_key='(week, token)',
  partition_by='toStartOfMonth(week)',
  tags=['production','intermediate','bridges']
) }}

SELECT
  toStartOfWeek(date) AS week,
  token,
  sumIf(volume_usd, direction='in')  - sumIf(volume_usd, direction='out') AS netflow_usd_week
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < toStartOfWeek(today())
{{ apply_monthly_incremental_filter('date', 'week', 'true') }}
GROUP BY week, token