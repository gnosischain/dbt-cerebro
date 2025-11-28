{{ config(materialized='view', tags=['production','bridges', 'tier1', 'api: token_netflow_by_bridge_d']) }}

WITH base AS (
  SELECT date, bridge, token, value
  FROM {{ ref('fct_bridges_token_netflow_daily_by_bridge') }}
),
all_rows AS (
  SELECT
    date,
    'All'   AS bridge,
    token,
    sum(value) AS value
  FROM base
  GROUP BY date, token
),
unioned AS (
  SELECT date, bridge, token, value FROM base
  UNION ALL
  SELECT date, bridge, token, value FROM all_rows
)
SELECT
  date,
  bridge,
  token,
  value,
  multiIf(bridge = 'All', 0, 1) AS bridge_order
FROM unioned
ORDER BY date, bridge_order, bridge, token