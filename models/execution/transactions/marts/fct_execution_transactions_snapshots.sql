{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transactions_info_daily') }}
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
    sumIf(n_txs, success = 1)          AS total_txs,
    sumIf(fee_native_sum, success = 1) AS fee_native_sum,
    sumIf(fee_usd_sum,    success = 1) AS fee_usd_sum
  FROM {{ ref('int_execution_transactions_info_daily') }}
  GROUP BY date
),

aa AS (
  SELECT
    date,
    active_accounts AS aa_cnt
  FROM {{ ref('fct_execution_transactions_active_accounts_daily') }}
),

joined AS (
  SELECT
    d.date,
    d.total_txs,
    d.fee_native_sum,
    d.fee_usd_sum,
    a.aa_cnt
  FROM d
  LEFT JOIN aa a ON a.date = d.date
),

curr_win AS (
  SELECT
    r.window,
    j.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN joined j
  WHERE
       (r.window = 'All'  AND j.date <= w.max_date)
    OR (r.window != 'All' AND j.date > subtractDays(w.max_date, r.days) AND j.date <= w.max_date)
),

prev_win AS (
  SELECT
    r.window,
    j.*
  FROM rng r
  CROSS JOIN wd w
  CROSS JOIN joined j
  WHERE
    r.window != 'All'
    AND j.date >  subtractDays(w.max_date, 2 * r.days)
    AND j.date <= subtractDays(w.max_date, r.days)
),

curr_agg AS (
  SELECT
    window,
    sum(total_txs)      AS txs,
    sum(fee_native_sum) AS fee_native,
    sum(fee_usd_sum)    AS fee_usd,
    sum(aa_cnt)         AS aa_sum
  FROM curr_win
  GROUP BY window
),

prev_agg AS (
  SELECT
    window,
    sum(total_txs)      AS txs,
    sum(fee_native_sum) AS fee_native,
    sum(fee_usd_sum)    AS fee_usd,
    sum(aa_cnt)         AS aa_sum
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
  'FeesUSD',
  c.window,
  toFloat64(c.fee_usd),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.fee_usd / nullIf(p.fee_usd, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p ON p.window = c.window

UNION ALL
SELECT
  'ActiveAccounts',
  c.window,
  toFloat64(c.aa_sum),
  CASE WHEN c.window = 'All' THEN NULL
       ELSE round((coalesce(c.aa_sum / nullIf(p.aa_sum, 0), 0) - 1) * 100, 1)
  END
FROM curr_agg c
LEFT JOIN prev_agg p ON p.window = c.window