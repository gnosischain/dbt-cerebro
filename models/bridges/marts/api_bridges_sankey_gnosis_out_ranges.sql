{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH mx AS (
  SELECT max(date) AS d
  FROM {{ ref('fct_bridges_edges_source_bridge_daily') }}
),
ranges AS (
  SELECT '1D'  AS range, mx.d                             AS start_d, mx.d AS end_d, 1 AS range_order FROM mx
  UNION ALL SELECT '7D',  subtractDays(mx.d,  6), mx.d, 2 FROM mx
  UNION ALL SELECT '30D', subtractDays(mx.d, 29), mx.d, 3 FROM mx
  UNION ALL SELECT '90D', subtractDays(mx.d, 89), mx.d, 4 FROM mx
  UNION ALL SELECT 'All', toDate('2024-01-01'), mx.d,   5 FROM mx
),

left_edges AS ( 
  SELECT
    r.range,
    r.range_order,
    'gnosis'         AS source,
    e.bridge         AS target,
    sum(e.value)     AS value
  FROM {{ ref('fct_bridges_edges_source_bridge_daily') }} e
  JOIN ranges r ON e.date BETWEEN r.start_d AND r.end_d
  WHERE lower(e.source_chain) = 'gnosis'
  GROUP BY r.range, r.range_order, target
  HAVING value > 0
),

right_edges AS ( 
  SELECT
    r.range,
    r.range_order,
    e.bridge         AS source,
    e.dest_chain     AS target,
    sum(e.value)     AS value
  FROM {{ ref('fct_bridges_edges_bridge_dest_daily') }} e
  JOIN ranges r ON e.date BETWEEN r.start_d AND r.end_d
  WHERE lower(e.dest_chain) != 'gnosis'
  GROUP BY r.range, r.range_order, source, target
  HAVING value > 0
)

SELECT range, source, target, value
FROM (
  SELECT * FROM left_edges
  UNION ALL
  SELECT * FROM right_edges
)
ORDER BY
  range_order,
  value DESC,
  source ASC,
  target ASC