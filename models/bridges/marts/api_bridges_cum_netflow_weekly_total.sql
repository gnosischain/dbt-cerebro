{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH b AS (
  SELECT week, sum(netflow_usd_week) AS netflow_usd_week
  FROM {{ ref('fct_bridges_netflow_weekly_by_bridge') }}
  GROUP BY week
)
SELECT
  week AS date,
  sum(netflow_usd_week) OVER (ORDER BY week
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value
FROM b
ORDER BY date