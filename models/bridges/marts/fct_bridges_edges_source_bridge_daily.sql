{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(date, source_chain, bridge)',
  unique_key='(date, source_chain, bridge)',
  partition_by='toStartOfMonth(date)',
  settings={'allow_nullable_key': 1},
  tags=['production','bridges','fct']
) }}

SELECT
  date,
  source_chain,
  bridge,
  sum(volume_usd) AS value
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < today()
{{ apply_monthly_incremental_filter('date','date','true') }}
GROUP BY date, source_chain, bridge