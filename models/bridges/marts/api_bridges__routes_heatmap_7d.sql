{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH mx AS (
  SELECT max(date) AS d
  FROM {{ ref('fct_bridges_routes_daily') }}
)

SELECT
  source_chain AS x,
  dest_chain   AS y,
  sum(volume_usd) AS value
FROM {{ ref('fct_bridges_routes_daily') }}, mx
WHERE date >= subtractDays(mx.d, 7)
GROUP BY x, y
ORDER BY value DESC