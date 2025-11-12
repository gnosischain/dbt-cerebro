

WITH mx AS (SELECT max(date) AS d FROM `dbt`.`fct_bridges_sankey_edges_token_daily`),
mn AS (SELECT min(date) AS m FROM `dbt`.`fct_bridges_sankey_edges_token_daily`),
ranges AS (
  SELECT '1D'  AS range, mx.d                       AS start_d, mx.d AS end_d, 1 AS range_order FROM mx
  UNION ALL SELECT '7D',  subtractDays(mx.d,  6), mx.d, 2 FROM mx
  UNION ALL SELECT '30D', subtractDays(mx.d, 29), mx.d, 3 FROM mx
  UNION ALL SELECT '90D', subtractDays(mx.d, 89), mx.d, 4 FROM mx
  UNION ALL SELECT 'All', mn.m, mx.d, 5 FROM mn, mx
)

SELECT
  r.range,
  e.source,
  e.target,
  sum(e.value) AS value
FROM `dbt`.`fct_bridges_sankey_edges_token_daily` e
JOIN ranges r ON e.date BETWEEN r.start_d AND r.end_d
WHERE e.direction = 'in'
GROUP BY r.range, e.source, e.target, r.range_order
HAVING value > 0
ORDER BY r.range_order, value DESC, e.source ASC, e.target ASC