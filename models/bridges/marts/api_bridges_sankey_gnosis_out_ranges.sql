{{ config(materialized='view', tags=['production','bridges','api']) }}

WITH mx AS (
  SELECT max(date) AS d FROM {{ ref('int_bridges_flows_daily') }}
),
mn AS (
  SELECT min(date) AS m FROM {{ ref('int_bridges_flows_daily') }}
),
ranges AS (
  SELECT '1D'  AS range, mx.d                       AS start_d, mx.d AS end_d, 1 AS range_order FROM mx
  UNION ALL SELECT '7D',  subtractDays(mx.d,  6), mx.d, 2 FROM mx
  UNION ALL SELECT '30D', subtractDays(mx.d, 29), mx.d, 3 FROM mx
  UNION ALL SELECT '90D', subtractDays(mx.d, 89), mx.d, 4 FROM mx
  UNION ALL SELECT 'All', mn.m, mx.d, 5 FROM mn, mx
),

left_edges AS ( 
  SELECT
    r.range,
    r.range_order,
    'gnosis'        AS source,
    e.bridge        AS target,
    sum(e.volume_usd) AS value
  FROM {{ ref('int_bridges_flows_daily') }} e
  JOIN ranges r ON e.date BETWEEN r.start_d AND r.end_d
  WHERE lower(e.source_chain) = 'gnosis'
  GROUP BY r.range, r.range_order, target
),

right_edges AS ( 
  SELECT
    r.range,
    r.range_order,
    e.bridge                    AS source,
    lower(trim(e.dest_chain))   AS target,
    sum(e.volume_usd)           AS value
  FROM {{ ref('int_bridges_flows_daily') }} e
  JOIN ranges r ON e.date BETWEEN r.start_d AND r.end_d
  WHERE lower(e.source_chain) = 'gnosis'
    AND lower(e.dest_chain)  != 'gnosis'
  GROUP BY r.range, r.range_order, source, target
)

SELECT range, source, target, value
FROM (
  SELECT * FROM left_edges
  UNION ALL
  SELECT * FROM right_edges
)
ORDER BY range_order, value DESC, source ASC, target ASC