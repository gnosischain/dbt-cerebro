

WITH mx AS (SELECT max(date) AS d FROM `dbt`.`fct_bridges_sankey_edges_token_daily`)

SELECT token, source, target, sum(value) AS value
FROM `dbt`.`fct_bridges_sankey_edges_token_daily`, mx
WHERE direction = 'out'
  AND date BETWEEN subtractDays(mx.d, 6) AND mx.d
GROUP BY token, source, target
HAVING value > 0
ORDER BY token, value DESC, source ASC, target ASC