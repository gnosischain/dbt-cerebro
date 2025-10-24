{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH mx AS (
  SELECT max(date) AS d
  FROM {{ ref('fct_bridges_edges_source_bridge_daily') }}
)

SELECT
  date,
  source_chain AS source,
  bridge       AS target,
  value
FROM {{ ref('fct_bridges_edges_source_bridge_daily') }}, mx
WHERE date >= subtractDays(mx.d, 7)

UNION ALL

SELECT
  date,
  bridge     AS source,
  dest_chain AS target,
  value
FROM {{ ref('fct_bridges_edges_bridge_dest_daily') }}, mx
WHERE date >= subtractDays(mx.d, 7)