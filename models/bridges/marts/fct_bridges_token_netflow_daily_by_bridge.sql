{{ config(
  materialized = 'table',
  engine       = 'MergeTree()',
  order_by     = '(date, bridge, token)',
  partition_by = 'toStartOfMonth(date)',
  settings     = {'allow_nullable_key': 1},
  tags         = ['production','bridges','fct']
) }}

SELECT
  date,
  trim(bridge)           AS bridge,
  upper(trim(token))     AS token,
  sum(net_usd)           AS value
FROM {{ ref('int_bridges_flows_daily') }}
WHERE date < today()               
GROUP BY date, bridge, token
ORDER BY date, bridge, token