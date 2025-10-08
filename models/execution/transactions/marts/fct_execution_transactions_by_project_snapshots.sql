{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),
rng AS (
  SELECT '1D'  AS window,  1   AS days UNION ALL
  SELECT '7D'  AS window,  7   AS days UNION ALL
  SELECT '30D' AS window,  30  AS days UNION ALL
  SELECT '90D' AS window,  90  AS days UNION ALL
  SELECT 'All' AS window,  1000000 AS days
),

d AS (
  SELECT
    date,
    project,
    tx_count,
    fee_native_sum,
    fee_usd_sum,
    ua_bitmap_state       
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),

curr_win AS (
  SELECT
    r.window,
    d.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN d
  WHERE
       (r.window = 'All'  AND d.date <= w.max_date)
    OR (r.window != 'All' AND d.date > subtractDays(w.max_date, r.days) AND d.date <= w.max_date)
),

prev_win AS (
  SELECT
    r.window,
    d.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN d
  WHERE
    r.window != 'All'
    AND d.date >  subtractDays(w.max_date, 2 * r.days)
    AND d.date <= subtractDays(w.max_date, r.days)
),

curr_agg AS (
  SELECT
    window,
    project,
    sum(tx_count)         AS txs,
    sum(fee_native_sum)   AS fee_native,
    sum(fee_usd_sum)      AS fee_usd,
    groupBitmapMerge(ua_bitmap_state) AS aa_uniques   -- UInt64
  FROM curr_win
  GROUP BY window, project
),
prev_agg AS (
  SELECT
    window,
    project,
    sum(tx_count)         AS txs,
    sum(fee_native_sum)   AS fee_native,
    sum(fee_usd_sum)      AS fee_usd,
    groupBitmapMerge(ua_bitmap_state) AS aa_uniques
  FROM prev_win
  GROUP BY window, project
)

SELECT
  'Transactions'          AS label,
  c.window                AS window,
  c.project               AS bucket,
  toFloat64(c.txs)        AS value,
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.txs / nullIf(p.txs, 0), 0) - 1) * 100, 1)
  END                     AS change_pct
FROM curr_agg c
LEFT JOIN prev_agg p
  ON p.window = c.window AND p.project = c.project

UNION ALL

SELECT
  'FeesUSD',
  c.window,
  c.project,
  toFloat64(c.fee_usd),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.fee_usd / nullIf(p.fee_usd, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p
  ON p.window = c.window AND p.project = c.project

UNION ALL
SELECT
  'FeesNative',
  c.window,
  c.project,
  toFloat64(c.fee_native),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.fee_native / nullIf(p.fee_native, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p
  ON p.window = c.window AND p.project = c.project

UNION ALL
SELECT
  'ActiveAccounts',
  c.window,
  c.project,
  toFloat64(c.aa_uniques) AS value,
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.aa_uniques / nullIf(p.aa_uniques, 0), 0) - 1) * 100, 1)
  END                     AS change_pct
FROM curr_agg c
LEFT JOIN prev_agg p
  ON p.window = c.window AND p.project = c.project