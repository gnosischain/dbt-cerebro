{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH mx AS (
  SELECT max(date) AS d
  FROM {{ ref('fct_bridges_routes_daily') }}
)

SELECT
  source_chain AS source,
  dest_chain   AS target,
  sum(volume_usd) AS value
FROM {{ ref('fct_bridges_routes_daily') }}, mx
WHERE date >= subtractDays(mx.d, 7)
  AND source_chain != dest_chain
GROUP BY source, target
ORDER BY value DESC