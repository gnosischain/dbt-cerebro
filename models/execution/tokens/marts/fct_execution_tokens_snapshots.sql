{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, label)',
    tags=['production','execution','tokens']
  )
}}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_tokens_value_daily') }}
),
rng AS (
  SELECT '1D'  AS window,  1  AS days UNION ALL
  SELECT '7D'  AS window,  7  AS days UNION ALL
  SELECT '30D' AS window,  30 AS days UNION ALL
  SELECT '90D' AS window,  90 AS days
),
bounds AS (
  SELECT
    r.window,
    r.days,
    w.max_date,
    subtractDays(w.max_date, r.days)        AS curr_start,
    w.max_date                              AS curr_end,
    subtractDays(w.max_date, 2 * r.days)    AS prev_start,
    subtractDays(w.max_date, r.days)        AS prev_end
  FROM rng r
  CROSS JOIN wd w
),

curr_win AS (
  SELECT
    b.window,
    sum(d.supply)                    AS supply,
    sum(d.holders)                  AS holders,
    sum(d.value_usd)                AS value_usd,
    sum(d.volume_usd)                AS volume_usd,
    sum(d.transfer_count)            AS transfer_count,
    toUInt64(groupBitmapMerge(d.ua_bitmap_state)) AS active_senders
  FROM {{ ref('int_execution_tokens_value_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.curr_start
   AND d.date <= b.curr_end
  GROUP BY b.window
),

prev_win AS (
  SELECT
    b.window,
    sum(d.supply)                    AS supply,
    sum(d.holders)                  AS holders,
    sum(d.value_usd)                AS value_usd,
    sum(d.volume_usd)                AS volume_usd,
    sum(d.transfer_count)            AS transfer_count,
    toUInt64(groupBitmapMerge(d.ua_bitmap_state)) AS active_senders
  FROM {{ ref('int_execution_tokens_value_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.prev_start
   AND d.date <= b.prev_end
  GROUP BY b.window
),

latest_per_token AS (
  SELECT
    token_address,
    argMax(supply, date)          AS supply,
    toUInt64(argMax(holders, date)) AS holders,
    argMax(value_usd, date)       AS value_usd
  FROM {{ ref('int_execution_tokens_value_daily') }}
  WHERE date < today()
  GROUP BY token_address
),

latest_values AS (
  SELECT
    sum(supply)                      AS supply,
    sum(holders)                    AS holders,
    sum(value_usd)                  AS value_usd
  FROM latest_per_token
),

alltime_flows AS (
  SELECT
    sum(volume_usd)                                AS volume_usd,
    sum(transfer_count)                            AS transfer_count,
    toUInt64(groupBitmapMerge(ua_bitmap_state))    AS active_senders
  FROM {{ ref('int_execution_tokens_value_daily') }}
  WHERE date < today()
),

curr_all AS (
  SELECT
    'All' AS window,
    l.supply,
    l.holders,
    l.value_usd,
    f.volume_usd,
    f.transfer_count,
    f.active_senders
  FROM latest_values l
  CROSS JOIN alltime_flows f
),

curr AS (
  SELECT * FROM curr_win
  UNION ALL
  SELECT * FROM curr_all
)

SELECT
  'Supply'            AS label,
  c.window            AS window,
  toFloat64(c.supply) AS value,
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.supply / nullIf(p.supply, 0), 0) - 1) * 100, 1)
  END AS change_pct
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL

SELECT
  'Holders',
  c.window,
  toFloat64(c.holders),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.holders / nullIf(p.holders, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL

SELECT
  'ValueUSD',
  c.window,
  round(toFloat64(c.value_usd), 2),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.value_usd / nullIf(p.value_usd, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL

SELECT
  'VolumeUSD',
  c.window,
  round(toFloat64(c.volume_usd), 2),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.volume_usd / nullIf(p.volume_usd, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL

SELECT
  'ActiveSenders',
  c.window,
  toFloat64(c.active_senders),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.active_senders / nullIf(p.active_senders, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

