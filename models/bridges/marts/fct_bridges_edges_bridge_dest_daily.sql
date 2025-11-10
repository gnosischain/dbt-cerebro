{{ config(
  materialized='table',
  engine='MergeTree()',
  order_by='(date, bridge, dest_chain)',
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
GROUP BY date, bridge, dest_chain