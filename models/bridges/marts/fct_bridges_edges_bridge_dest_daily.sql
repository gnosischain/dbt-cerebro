{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(date, bridge, dest_chain)',
  unique_key='(date, bridge, dest_chain)',
  partition_by='toStartOfMonth(date)',
  settings={'allow_nullable_key': 1},
  tags=['production','bridges','fct']
) }}

SELECT
  date,
  bridge,
  dest_chain,
  sum(volume_usd) AS value
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < today()
{{ apply_monthly_incremental_filter('date','date','true') }}
GROUP BY date, bridge, dest_chain