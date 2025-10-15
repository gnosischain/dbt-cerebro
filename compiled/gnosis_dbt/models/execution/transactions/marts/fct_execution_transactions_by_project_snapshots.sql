

WITH wd AS (
  SELECT max(date) AS max_date
  FROM `dbt`.`int_execution_transactions_by_project_daily`
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
    d.project,
    sum(d.tx_count)                     AS txs,
    sum(d.fee_native_sum)               AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state) AS aa_uniques
  FROM `dbt`.`int_execution_transactions_by_project_daily` d
  INNER JOIN bounds b
    ON d.date >  b.curr_start
   AND d.date <= b.curr_end
  GROUP BY b.window, d.project
),
prev_win AS (
  SELECT
    b.window,
    d.project,
    sum(d.tx_count)                     AS txs,
    sum(d.fee_native_sum)               AS fee_native,
    groupBitmapMerge(d.ua_bitmap_state) AS aa_uniques
  FROM `dbt`.`int_execution_transactions_by_project_daily` d
  INNER JOIN bounds b
    ON d.date >  b.prev_start
   AND d.date <= b.prev_end
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
  FROM `dbt`.`int_execution_transactions_by_project_alltime_state` a
  GROUP BY a.project
),

joined AS (
  SELECT
    c.window,
    c.project,
    c.txs        AS txs_curr,
    p.txs        AS txs_prev,
    c.fee_native AS fee_curr,
    p.fee_native AS fee_prev,
    c.aa_uniques AS aa_curr,
    p.aa_uniques AS aa_prev
  FROM curr_win c
  LEFT JOIN prev_win p
    ON p.window = c.window AND p.project = c.project
),

all_windows AS (
  SELECT window, project, txs, fee_native, aa_uniques FROM curr_win
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
    round((coalesce(j.txs_curr / nullIf(j.txs_prev, 0), 0) - 1) * 100, 1)
  )                         AS change_pct
FROM all_windows w
LEFT JOIN joined j
  ON j.window = w.window AND j.project = w.project

UNION ALL
SELECT
  'FeesNative',
  w.window,
  w.project,
  round(toFloat64(w.fee_native), 6),
  multiIf(
    w.window = 'All', NULL,
    round((coalesce(j.fee_curr / nullIf(j.fee_prev, 0), 0) - 1) * 100, 1)
  )
FROM all_windows w
LEFT JOIN joined j
  ON j.window = w.window AND j.project = w.project

UNION ALL
SELECT
  'ActiveAccounts',
  w.window,
  w.project,
  toFloat64(w.aa_uniques),
  multiIf(
    w.window = 'All', NULL,
    round((coalesce(j.aa_curr / nullIf(j.aa_prev, 0), 0) - 1) * 100, 1)
  )
FROM all_windows w
LEFT JOIN joined j
  ON j.window = w.window AND j.project = w.project