{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, bucket, label)',
    tags=['production','execution','transactions'],
    pre_hook=["SET join_algorithm = 'hash'"],
    post_hook=["SET join_algorithm = 'default'"]
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

/* single scan: curr + prev periods via conditional aggregation */
agg AS (
  SELECT
    b.window,
    d.project,
    sumIf(d.tx_count, d.date > b.curr_start AND d.date <= b.curr_end)                       AS txs_curr,
    sumIf(d.tx_count, d.date > b.prev_start AND d.date <= b.prev_end)                       AS txs_prev,
    sumIf(d.fee_native_sum, d.date > b.curr_start AND d.date <= b.curr_end)                  AS fee_curr,
    sumIf(d.fee_native_sum, d.date > b.prev_start AND d.date <= b.prev_end)                  AS fee_prev,
    groupBitmapMergeIf(d.ua_bitmap_state, d.date > b.curr_start AND d.date <= b.curr_end)    AS aa_curr,
    groupBitmapMergeIf(d.ua_bitmap_state, d.date > b.prev_start AND d.date <= b.prev_end)    AS aa_prev
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  INNER JOIN bounds b
    ON d.date > b.prev_start
   AND d.date <= b.curr_end
  GROUP BY b.window, d.project
),

/* all-time from AMT */
curr_all AS (
  SELECT
    'All' AS window,
    a.project,
    sumMerge(a.txs_state)             AS txs,
    sumMerge(a.fee_state)             AS fee_native,
    groupBitmapMerge(a.aa_state)      AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_alltime_state') }} a
  GROUP BY a.project
),

all_windows AS (
  SELECT window, project, txs_curr AS txs, fee_curr AS fee_native, aa_curr AS aa_uniques FROM agg
  UNION ALL
  SELECT window, project, txs, fee_native, aa_uniques FROM curr_all
)

SELECT
  'Transactions'            AS label,
  w.window                  AS window,
  w.project                 AS bucket,
  toFloat64(w.txs)          AS value,
  multiIf(
    w.window = 'All', NULL,
    round((coalesce(a.txs_curr / nullIf(a.txs_prev, 0), 0) - 1) * 100, 1)
  )                         AS change_pct
FROM all_windows w
LEFT JOIN agg a
  ON a.window = w.window AND a.project = w.project

UNION ALL
SELECT
  'FeesNative',
  w.window,
  w.project,
  round(toFloat64(w.fee_native), 6),
  multiIf(
    w.window = 'All', NULL,
    round((coalesce(a.fee_curr / nullIf(a.fee_prev, 0), 0) - 1) * 100, 1)
  )
FROM all_windows w
LEFT JOIN agg a
  ON a.window = w.window AND a.project = w.project

UNION ALL
SELECT
  'ActiveAccounts',
  w.window,
  w.project,
  toFloat64(w.aa_uniques),
  multiIf(
    w.window = 'All', NULL,
    round((coalesce(a.aa_curr / nullIf(a.aa_prev, 0), 0) - 1) * 100, 1)
  )
FROM all_windows w
LEFT JOIN agg a
  ON a.window = w.window AND a.project = w.project