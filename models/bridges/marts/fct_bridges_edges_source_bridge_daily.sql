{{ config(
  materialized='table',
  engine='MergeTree()',
  order_by='(date, source_chain, bridge)',
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
GROUP BY date, source_chain, bridge