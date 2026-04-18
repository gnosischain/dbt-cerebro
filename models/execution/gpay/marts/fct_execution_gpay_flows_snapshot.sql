{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(window, days, symbol, from_label, to_label)',
    tags=['production','execution','gpay'],
    pre_hook=["SET join_use_nulls = 1", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_use_nulls = 0", "SET join_algorithm = 'default'"]
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

gpay_wallets AS (
  SELECT address
  FROM {{ ref('int_execution_gpay_wallets') }}
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

base_addresses AS (
  SELECT DISTINCT address FROM (
    SELECT "from" AS address FROM base
    UNION ALL
    SELECT "to" AS address FROM base
  )
),

base_date_symbols AS (
  SELECT DISTINCT date, symbol FROM base
),

lbl_ranked AS (
  SELECT
    t1.address,
    IF(t2.address = t1.address, 'gpay', t1.project) AS project,
    t1.introduced_at,
    row_number() OVER (
      PARTITION BY t1.address
      ORDER BY t1.introduced_at DESC, t1.project DESC
    ) AS rn
  FROM {{ ref('int_crawlers_data_labels') }} t1
  LEFT JOIN gpay_wallets t2
    ON t2.address = t1.address
  WHERE t1.address IN (SELECT address FROM base_addresses)
),

lbl AS (
  SELECT
    address,
    project
  FROM lbl_ranked
  WHERE rn = 1
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
  WHERE (t1.date, t1.symbol) IN (SELECT date, symbol FROM base_date_symbols)
),

enriched AS (
  SELECT
    t1.date AS date,
    t1.symbol AS symbol,
    ifNull(t2.project, 'Unknown') AS from_label,
    ifNull(t3.project, 'Unknown') AS to_label,
    t1.amount_raw / POWER(10, t4.decimals) * t4.price AS amount_usd,
    t1.transfer_count AS transfer_count
  FROM base t1
  LEFT JOIN lbl t2
    ON t2.address = t1."from"
  LEFT JOIN lbl t3
    ON t3.address = t1."to"
  ANY LEFT JOIN prices t4
    ON t4.date = t1.date
   AND t4.symbol = t1.symbol
),

final AS (
  SELECT
    r.window AS window,
    r.days AS days,
    e.symbol AS symbol,
    e.from_label AS from_label,
    e.to_label AS to_label,
    SUM(e.amount_usd) AS amount_usd,
    SUM(e.transfer_count) AS tf_cnt
  FROM enriched e
  CROSS JOIN rng r
  WHERE e.date > subtractDays((SELECT max_date FROM wd), r.days)
    AND e.date <= (SELECT max_date FROM wd)
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  window,
  days,
  symbol,
  If((symbol = 'EURe' OR symbol = 'GBPe') AND from_label = 'Null/Burn', 'Bank', from_label) AS from_label,
  If((symbol = 'EURe' OR symbol = 'GBPe') AND to_label = 'Null/Burn', 'Bank', to_label) AS to_label,
  amount_usd,
  tf_cnt
FROM final
