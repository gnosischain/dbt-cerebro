{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(week, bridge)',
  unique_key='(week, bridge)',
  partition_by='toStartOfMonth(week)',
  tags=['production','intermediate','bridges']
) }}

SELECT
  toStartOfWeek(date) AS week,
  bridge,
  sumIf(volume_usd, direction='in')  AS inflow_usd,
  sumIf(volume_usd, direction='out') AS outflow_usd
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < toStartOfWeek(today())
{{ apply_monthly_incremental_filter('date', 'week', 'true') }}
GROUP BY week, bridge