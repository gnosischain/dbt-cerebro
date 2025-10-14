{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, label)',
    tags=['production','execution','transactions']
  )
}}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
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
    sum(d.tx_count)                     AS txs,
    sum(d.fee_native_sum)               AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state) AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.curr_start
   AND d.date <= b.curr_end
  GROUP BY b.window
),
prev_win AS (
  SELECT
    b.window,
    sum(d.tx_count)                     AS txs,
    sum(d.fee_native_sum)               AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state) AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.prev_start
   AND d.date <= b.prev_end
  GROUP BY b.window
),

curr_all AS (
  SELECT
    'All' AS window,
    sumMerge(a.txs_state)           AS txs,
    sumMerge(a.fee_state)           AS fee_native,
    groupBitmapMerge(a.aa_state)    AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_alltime_state') }} a
),

curr AS (
  SELECT * FROM curr_win
  UNION ALL
  SELECT * FROM curr_all
)

SELECT
  'Transactions'            AS label,
  c.window                  AS window,
  toFloat64(c.txs)          AS value,
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.txs / nullIf(p.txs, 0), 0) - 1) * 100, 1)
  END AS change_pct
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL
SELECT
  'FeesNative',
  c.window,
  round(toFloat64(c.fee_native), 2),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.fee_native / nullIf(p.fee_native, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window

UNION ALL
SELECT
  'ActiveAccounts',
  c.window,
  toFloat64(c.aa_uniques),
  CASE
    WHEN c.window = 'All' THEN NULL
    ELSE round((coalesce(c.aa_uniques / nullIf(p.aa_uniques, 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_win p ON p.window = c.window