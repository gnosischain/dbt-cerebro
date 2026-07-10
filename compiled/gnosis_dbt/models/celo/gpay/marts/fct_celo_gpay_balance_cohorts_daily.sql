

-- Per-day holder distribution across balance buckets, by token, in both USD
-- and native units. Mirrors fct_execution_gpay_balance_cohorts_daily.
-- Reads the dense per-Safe balances so a holder's bucket is correct on every
-- day, not only on days they transacted. Zero / negative balances are
-- excluded (a Safe holding nothing is not a "holder"). USDC / USDT only.
WITH balances_base AS (
    SELECT
        date,
        safe_address AS address,
        token_symbol AS symbol,
        balance,
        balance_usd
    FROM `dbt`.`fct_celo_gpay_balances_safe_daily`
    WHERE token_symbol IN ('USDC', 'USDT')
),

bucketed_usd AS (
    SELECT
        date,
        address,
        symbol,
        balance,
        balance_usd,
        'usd' AS cohort_unit,
        CASE
            WHEN balance_usd <       10 THEN '0-10'
            WHEN balance_usd <      100 THEN '10-100'
            WHEN balance_usd <     1000 THEN '100-1K'
            WHEN balance_usd <    10000 THEN '1K-10K'
            WHEN balance_usd <   100000 THEN '10K-100K'
            WHEN balance_usd <  1000000 THEN '100K-1M'
            ELSE                         '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance_usd IS NOT NULL
      AND balance_usd > 0
),

bucketed_native AS (
    SELECT
        date,
        address,
        symbol,
        balance,
        balance_usd,
        'native' AS cohort_unit,
        CASE
            WHEN balance <       10 THEN '0-10'
            WHEN balance <      100 THEN '10-100'
            WHEN balance <     1000 THEN '100-1K'
            WHEN balance <    10000 THEN '1K-10K'
            WHEN balance <   100000 THEN '10K-100K'
            WHEN balance <  1000000 THEN '100K-1M'
            ELSE                     '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance > 0
),

bucketed AS (
    SELECT * FROM bucketed_usd
    UNION ALL
    SELECT * FROM bucketed_native
)

SELECT
    date,
    symbol,
    cohort_unit,
    balance_bucket,
    count()                               AS holders,
    round(toFloat64(sum(balance)), 6)     AS value_native,
    round(toFloat64(sum(balance_usd)), 2) AS value_usd
FROM bucketed
GROUP BY date, symbol, cohort_unit, balance_bucket
ORDER BY date, symbol, cohort_unit, balance_bucket