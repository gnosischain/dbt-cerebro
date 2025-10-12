{{ config(
  materialized='table',
  engine='ReplacingMergeTree()',
  order_by='(window, bucket, label)',
  tags=['production','execution','transactions']
) }}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),
bounds AS (
  SELECT
    max_date,
    subtractDays(max_date, 7)   AS d7_start,
    subtractDays(max_date, 14)  AS d7_prev_start,
    subtractDays(max_date, 7)   AS d7_prev_end
  FROM wd
),
curr_7d AS (
  SELECT
    '7D' AS window,
    d.project,
    sum(d.tx_count)                       AS txs,
    sum(d.fee_native_sum)                 AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state)   AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  CROSS JOIN bounds b
  WHERE d.date >  b.d7_start
    AND d.date <= b.max_date
  GROUP BY window, d.project
),
prev_7d AS (
  SELECT
    '7D' AS window,
    d.project,
    sum(d.tx_count)                       AS txs,
    sum(d.fee_native_sum)                 AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state)   AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  CROSS JOIN bounds b
  WHERE d.date >  b.d7_prev_start
    AND d.date <= b.d7_prev_end
  GROUP BY window, d.project
),
curr_all AS (
  SELECT
    'All' AS window,
    d.project,
    sum(d.tx_count)                       AS txs,
    sum(d.fee_native_sum)                 AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state)   AS aa_uniques
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  CROSS JOIN wd
  WHERE d.date <= wd.max_date
  GROUP BY window, d.project
),
joined_7d AS (
  SELECT
    c.window,
    c.project,
    c.txs            AS txs_curr,
    p.txs            AS txs_prev,
    c.fee_native     AS fee_curr,
    p.fee_native     AS fee_prev,
    c.aa_uniques     AS aa_curr,
    p.aa_uniques     AS aa_prev
  FROM curr_7d c
  LEFT JOIN prev_7d p
    ON p.window = c.window AND p.project = c.project
),
curr_all_windows AS (
  SELECT window, project, txs, fee_native, aa_uniques FROM curr_7d
  UNION ALL
  SELECT window, project, txs, fee_native, aa_uniques FROM curr_all
)

SELECT
  'Transactions'                AS label,
  w.window                      AS window,
  w.project                     AS bucket,
  toFloat64(w.txs)              AS value,
  multiIf(
    w.window = 'All', NULL,  
    w.window = '7D', round((coalesce(j.txs_curr / nullIf(j.txs_prev, 0), 0) - 1) * 100, 1),
    NULL
  )                             AS change_pct
FROM curr_all_windows w
LEFT JOIN joined_7d j
  ON j.window = w.window AND j.project = w.project

UNION ALL

SELECT
  'FeesNative',
  w.window,
  w.project,
  toFloat64(w.fee_native),
  multiIf(
    w.window = 'All', NULL,
    w.window = '7D', round((coalesce(j.fee_curr / nullIf(j.fee_prev, 0), 0) - 1) * 100, 1),
    NULL
  )
FROM curr_all_windows w
LEFT JOIN joined_7d j
  ON j.window = w.window AND j.project = w.project

UNION ALL

SELECT
  'ActiveAccounts',
  w.window,
  w.project,
  toFloat64(w.aa_uniques)       AS value,
  multiIf(
    w.window = 'All', NULL,
    w.window = '7D', round((coalesce(j.aa_curr / nullIf(j.aa_prev, 0), 0) - 1) * 100, 1),
    NULL
  )                             AS change_pct
FROM curr_all_windows w
LEFT JOIN joined_7d j
  ON j.window = w.window AND j.project = w.project