{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH m AS (
  SELECT month, sum(netflow_usd_month) AS netflow_usd_month
  FROM {{ ref('int_bridges_netflow_monthly_by_bridge') }}
  GROUP BY month
)
SELECT
  month AS date,
  sum(netflow_usd_month) OVER (
    ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS value
FROM m
ORDER BY date