{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(window, label)',
    pre_hook=["SET join_algorithm = 'hash'"],
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

-- Txs + fees per window (no bitmaps)
curr_win AS (
  SELECT
    b.window,
    sum(d.tx_count)       AS txs,
    sum(d.fee_native_sum) AS fee_native
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.curr_start
   AND d.date <= b.curr_end
  GROUP BY b.window
),
prev_win AS (
  SELECT
    b.window,
    sum(d.tx_count)       AS txs,
    sum(d.fee_native_sum) AS fee_native
  FROM {{ ref('int_execution_transactions_by_project_daily') }} d
  INNER JOIN bounds b
    ON d.date >  b.prev_start
   AND d.date <= b.prev_end
  GROUP BY b.window
),

-- Windowed active accounts via countDistinct (no bitmap merge)
curr_aa AS (
  SELECT
    b.window,
    toUInt64(countDistinct(a.address_hash)) AS aa_uniques
  FROM {{ ref('int_execution_transactions_daily_active_addresses') }} a
  INNER JOIN bounds b
    ON a.date >  b.curr_start
   AND a.date <= b.curr_end
  GROUP BY b.window
),
prev_aa AS (
  SELECT
    b.window,
    toUInt64(countDistinct(a.address_hash)) AS aa_uniques
  FROM {{ ref('int_execution_transactions_daily_active_addresses') }} a
  INNER JOIN bounds b
    ON a.date >  b.prev_start
   AND a.date <= b.prev_end
  GROUP BY b.window
),

-- All-time: simple sums + cumulative model for AA
curr_all_txs AS (
  SELECT
    sum(tx_count)       AS txs,
    sum(fee_native_sum) AS fee_native
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
),
curr_all_aa AS (
  SELECT cumulative_accounts AS aa_uniques
  FROM {{ ref('int_execution_transactions_cumulative_daily') }}
  ORDER BY date DESC
  LIMIT 1
),

-- Combine windowed + all-time
curr AS (
  SELECT
    c.window,
    c.txs,
    c.fee_native,
    ca.aa_uniques
  FROM curr_win c
  INNER JOIN curr_aa ca ON ca.window = c.window
  UNION ALL
  SELECT
    'All'               AS window,
    t.txs,
    t.fee_native,
    toUInt64(a.aa_uniques)
  FROM curr_all_txs t
  CROSS JOIN curr_all_aa a
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
  round(toFloat64(c.fee_native), 6),
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
    ELSE round((coalesce(toFloat64(c.aa_uniques) / nullIf(toFloat64(pa.aa_uniques), 0), 0) - 1) * 100, 1)
  END
FROM curr c
LEFT JOIN prev_aa pa ON pa.window = c.window
