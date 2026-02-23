{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, days, symbol, from_label, to_label)',
    pre_hook=[
          "SET join_use_nulls = 1"
        ],
    tags=['production','execution','gpay']
  )
}}

WITH wd AS (
  SELECT max(date) AS max_date
  FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
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
    subtractDays(w.max_date, r.days) AS curr_start,
    w.max_date AS curr_end
  FROM rng r
  CROSS JOIN wd w
),

gpay_wallets AS (
  SELECT address
  FROM {{ ref('stg_gpay__wallets') }}
),

lbl AS (
  SELECT
    address,
    IF(t2.address = t1.address, 'gpay',  project) AS  project
  FROM {{ ref('int_crawlers_data_labels') }} t1
  LEFT JOIN gpay_wallets t2
    ON t2.address = t1.address
),


prices AS (
  SELECT
      t1.date,
      t1.symbol,
      t1.price,
      t2.decimals
  FROM {{ ref('int_execution_token_prices_daily') }} t1
  INNER JOIN {{ ref('tokens_whitelist') }} t2
    ON t2.symbol = t1.symbol
   AND t1.date >= toDate(t2.date_start)
   AND (t2.date_end IS NULL OR t1.date < toDate(t2.date_end))
  WHERE t1.date >= subtractDays((SELECT max_date FROM wd), 100)
),

base AS (
  SELECT
    date,
    symbol,
    "from",
    "to",
    amount_raw,
    transfer_count
  FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
  PREWHERE date >= subtractDays((SELECT max_date FROM wd), 90)
  WHERE ("from" IN (SELECT address FROM gpay_wallets)
      OR "to"   IN (SELECT address FROM gpay_wallets))
),

final AS (
  SELECT
      b.window AS window
    , b.days AS days
    , t1.symbol AS symbol
    , ifNull(t2.project, 'Unknown') AS from_label
    , ifNull(t3.project, 'Unknown') AS to_label
    , SUM(t1.amount_raw / POWER(10,t4.decimals) * t4.price) AS amount_usd
    , SUM(t1.transfer_count) AS tf_cnt
  FROM base t1
  INNER JOIN bounds b
    ON t1.date >  b.curr_start
   AND t1.date <= b.curr_end
  ANY LEFT JOIN lbl t2
    ON t2.address = t1."from"
  ANY LEFT JOIN lbl t3
    ON t3.address = t1."to"
  ANY LEFT JOIN prices t4
    ON t4.date = t1.date
   AND t4.symbol = t1.symbol
  GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM final
