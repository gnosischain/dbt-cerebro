{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, balance_bucket)',
    tags=['production','execution','gpay']
  )
}}

WITH wallet_totals AS (
    SELECT
        date,
        address,
        sum(balance_usd) AS total_balance_usd
    FROM {{ ref('int_execution_gpay_balances_daily') }}
    WHERE balance_usd IS NOT NULL
      AND balance_usd > 0
    GROUP BY date, address
),

bucketed AS (
    SELECT
        date,
        address,
        total_balance_usd,
        CASE
            WHEN total_balance_usd <       10 THEN '0-10'
            WHEN total_balance_usd <      100 THEN '10-100'
            WHEN total_balance_usd <     1000 THEN '100-1K'
            WHEN total_balance_usd <    10000 THEN '1K-10K'
            WHEN total_balance_usd <   100000 THEN '10K-100K'
            WHEN total_balance_usd <  1000000 THEN '100K-1M'
            ELSE                               '1M+'
        END AS balance_bucket
    FROM wallet_totals
)

SELECT
    date,
    balance_bucket,
    count()                                    AS holders,
    round(toFloat64(sum(total_balance_usd)), 2) AS value_usd
FROM bucketed
GROUP BY date, balance_bucket
ORDER BY date, balance_bucket
