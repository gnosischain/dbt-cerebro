

WITH w AS (
  SELECT
    toStartOfWeek(date, 1) AS week,
    bridge,
    sum(net_usd)           AS netflow_usd_week
  FROM `dbt`.`int_bridges_flows_daily`
  WHERE date < toStartOfWeek(today(), 1)
  GROUP BY week, bridge
),
bounds AS (
  SELECT min(week) AS minw, max(week) AS maxw FROM w
),
calendar AS (
  SELECT toDate(addWeeks(minw, number)) AS week
  FROM bounds
  ARRAY JOIN range(dateDiff('week', minw, maxw) + 1) AS number
),
bridges AS (
  SELECT DISTINCT bridge FROM w
),
grid AS (
  SELECT b.bridge, c.week
  FROM bridges b
  CROSS JOIN calendar c
),
filled AS (
  SELECT
    g.week,
    g.bridge,
    coalesce(w.netflow_usd_week, 0) AS netflow_usd_week
  FROM grid g
  LEFT JOIN w
    ON w.week = g.week
   AND w.bridge = g.bridge
)
SELECT
  week,
  bridge,
  netflow_usd_week,
  sum(netflow_usd_week) OVER (
    PARTITION BY bridge
    ORDER BY week
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_netflow_usd
FROM filled
ORDER BY week, bridge