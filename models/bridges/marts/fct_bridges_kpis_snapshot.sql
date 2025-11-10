{{ config(
  materialized = 'table',
  engine = 'MergeTree()',
  order_by = '(as_of_date)',
  partition_by = 'toStartOfMonth(as_of_date)',
  settings = {'allow_nullable_key': 1},
  tags = ['production','bridges','fct']
) }}

WITH mx AS (
  SELECT max(date) AS d
  FROM {{ ref('int_bridges_flows_daily') }}
),
cum AS (
  SELECT
    sum(volume_usd) AS cum_vol_usd,
    sum(net_usd)    AS cum_net_usd,
    sum(txs)        AS cum_txs
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date <= mx.d
),
cur7 AS (
  SELECT
    sum(volume_usd) AS vol_7d,
    sum(net_usd)    AS net_7d,
    sum(txs)        AS txs_7d
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date BETWEEN subtractDays(mx.d, 6) AND mx.d
),
prev7 AS (
  SELECT
    sum(volume_usd) AS vol_prev_7d,
    sum(net_usd)    AS net_prev_7d,
    sum(txs)        AS txs_prev_7d
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date BETWEEN subtractDays(mx.d, 13) AND subtractDays(mx.d, 7)
),
bridges AS (
  SELECT uniqExact(trim(lower(bridge))) AS distinct_bridges
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date <= mx.d
),
chains_u AS (
  SELECT trim(lower(source_chain)) AS chain
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date <= mx.d AND lower(source_chain) != 'gnosis'
  UNION ALL
  SELECT trim(lower(dest_chain)) AS chain
  FROM {{ ref('int_bridges_flows_daily') }}, mx
  WHERE date <= mx.d AND lower(dest_chain) != 'gnosis'
),
chains AS (
  SELECT uniqExact(chain) AS distinct_chains
  FROM chains_u
)
SELECT
  mx.d AS as_of_date,
  cum.cum_vol_usd,
  cum.cum_net_usd,
  cum.cum_txs,
  cur7.vol_7d,
  cur7.net_7d,
  cur7.txs_7d,
  coalesce(prev7.vol_prev_7d, 0) AS vol_prev_7d,
  coalesce(prev7.net_prev_7d, 0) AS net_prev_7d,
  coalesce(prev7.txs_prev_7d, 0) AS txs_prev_7d,
  if(cur7.vol_7d = 0, NULL, cur7.net_7d / cur7.vol_7d) AS rate_7d,
  if(coalesce(prev7.vol_prev_7d,0) = 0, NULL, coalesce(prev7.net_prev_7d,0) / prev7.vol_prev_7d) AS rate_prev_7d,
  if(coalesce(prev7.vol_prev_7d,0) = 0, NULL, (cur7.vol_7d - coalesce(prev7.vol_prev_7d,0)) / prev7.vol_prev_7d) AS chg_vol_7d,
  if(coalesce(prev7.net_prev_7d,0) = 0, NULL, (cur7.net_7d - coalesce(prev7.net_prev_7d,0)) / prev7.net_prev_7d) AS chg_net_7d,
  if(
    coalesce(prev7.vol_prev_7d,0) = 0 OR coalesce(prev7.net_prev_7d,0) = 0 OR cur7.vol_7d = 0 OR cur7.net_7d = 0,
    NULL,
    ( (cur7.net_7d / cur7.vol_7d) - (prev7.net_prev_7d / prev7.vol_prev_7d) ) / (prev7.net_prev_7d / prev7.vol_prev_7d)
  ) AS chg_rate_7d,
  bridges.distinct_bridges,
  chains.distinct_chains
FROM mx, cum, cur7, prev7, bridges, chains