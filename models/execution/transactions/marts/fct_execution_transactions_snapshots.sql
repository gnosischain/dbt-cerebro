{{ config(materialized='table', tags=['production','execution','transactions']) }}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),
rng AS (
  SELECT '1D'  AS window,  1        AS days UNION ALL
  SELECT '7D'  AS window,  7        AS days UNION ALL
  SELECT '30D' AS window,  30       AS days UNION ALL
  SELECT '90D' AS window,  90       AS days UNION ALL
  SELECT 'All' AS window,  1000000  AS days
),

per_day AS (
  SELECT
    date,
    sum(tx_count)                          AS total_txs,
    sum(fee_native_sum)                    AS fee_native_sum,
    groupBitmapMergeState(ua_bitmap_state) AS ua_state_day
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  GROUP BY date
),

curr_win AS (
  SELECT r.window, p.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN per_day p
  WHERE
        (r.window = 'All'  AND p.date <= w.max_date)
     OR (r.window != 'All' AND p.date > subtractDays(w.max_date, r.days) AND p.date <= w.max_date)
),

prev_win AS (
  SELECT r.window, p.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN per_day p
  WHERE
    r.window != 'All'
    AND p.date >  subtractDays(w.max_date, 2 * r.days)
    AND p.date <= subtractDays(w.max_date, r.days)
),

curr_agg AS (
  SELECT
    window,
    sum(total_txs)               AS txs,
    sum(fee_native_sum)          AS fee_native,
    groupBitmapMerge(ua_state_day) AS aa_uniques
  FROM curr_win
  GROUP BY window
),
prev_agg AS (
  SELECT
    window,
    sum(total_txs)               AS txs,
    sum(fee_native_sum)          AS fee_native,
    groupBitmapMerge(ua_state_day) AS aa_uniques
  FROM prev_win
  GROUP BY window
)

SELECT
  'Transactions'           AS label,
  c.window                 AS window,
  toFloat64(c.txs)         AS value,
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.txs / nullIf(p.txs, 0), 0) - 1) * 100, 1)
  END                      AS change_pct
FROM curr_agg c
LEFT JOIN prev_agg p ON p.window = c.window

UNION ALL
SELECT
  'FeesNative',
  c.window,
  toFloat64(c.fee_native),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.fee_native / nullIf(p.fee_native, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p ON p.window = c.window

UNION ALL
SELECT
  'ActiveAccounts',
  c.window,
  toFloat64(c.aa_uniques),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.aa_uniques / nullIf(p.aa_uniques, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p ON p.window = c.window