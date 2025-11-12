

SELECT
  date,
  bridge                 AS bridge,
  token                  AS token,
  sum(net_usd)           AS value
FROM `dbt`.`int_bridges_flows_daily`
WHERE date < today()               
GROUP BY date, bridge, token
ORDER BY date, bridge, token