{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  engine='ReplacingMergeTree()',
  order_by='(week, bridge)',
  unique_key='(week, bridge)',
  partition_by='toStartOfMonth(week)',
  tags=['production','intermediate','bridges']
) }}

WITH w AS (
  SELECT
    toStartOfWeek(date)                      AS week,
    bridge                                   AS bridge,  
    sumIf(volume_usd, direction = 'in')      AS inflow_usd,
    sumIf(volume_usd, direction = 'out')     AS outflow_usd
  FROM {{ ref('int_bridges_flows_daily') }}
  WHERE date < toStartOfWeek(today())
  {{ apply_monthly_incremental_filter('date', 'week', 'true') }}
  GROUP BY week, bridge
),

n AS (
  SELECT
    week,
    bridge,
    inflow_usd - outflow_usd AS netflow_usd_week
  FROM w
),

bounds AS (
  SELECT min(week) AS minw, max(week) AS maxw FROM n
),
calendar AS (
  SELECT toDate(addWeeks(minw, number)) AS week
  FROM bounds
  ARRAY JOIN range(dateDiff('week', minw, maxw) + 1) AS number
),

bridges AS (
  SELECT DISTINCT bridge FROM n
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
    coalesce(n.netflow_usd_week, 0) AS netflow_usd_week
  FROM grid g
  LEFT JOIN n
    ON n.week = g.week
   AND n.bridge = g.bridge
),

final AS (
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
)

SELECT
  week,
  bridge,
  netflow_usd_week,
  cum_netflow_usd
FROM final
ORDER BY week, bridge