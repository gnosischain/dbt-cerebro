{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(date, source_chain, dest_chain)',
  unique_key='(date, source_chain, dest_chain)',
  partition_by='toStartOfMonth(date)',
  settings={'allow_nullable_key': 1},
  tags=['production','bridges','fct']
) }}

SELECT
  date,
  source_chain,
  dest_chain,
  sum(volume_usd) AS volume_usd,
  sum(net_usd)    AS net_usd,
  sum(txs)        AS txs
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < today()
{{ apply_monthly_incremental_filter('date','date','true') }}
GROUP BY date, source_chain, dest_chain