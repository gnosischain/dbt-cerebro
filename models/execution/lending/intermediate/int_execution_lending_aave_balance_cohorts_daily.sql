{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, reserve_address, cohort_unit, balance_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, reserve_address, cohort_unit, balance_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','yields','aave','balance_cohorts_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

balances_base AS (
    SELECT
        b.date AS date,
        b.reserve_address AS reserve_address,
        b.symbol AS symbol,
        b.user_address AS user_address,
        b.balance AS balance,
        b.balance_usd AS balance_usd
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} b
    WHERE b.date < today()
      AND b.user_address != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
),

bucketed_usd AS (
    SELECT
        date,
        reserve_address,
        symbol,
        user_address,
        balance,
        balance_usd,
        'usd' AS cohort_unit,
        CASE
            WHEN balance_usd <     0.01       THEN '0-0.01'
            WHEN balance_usd <      0.1       THEN '0.01-0.1'
            WHEN balance_usd <        1       THEN '0.1-1'
            WHEN balance_usd <       10       THEN '1-10'
            WHEN balance_usd <      100       THEN '10-100'
            WHEN balance_usd <     1000       THEN '100-1k'
            WHEN balance_usd <    10000       THEN '1k-10k'
            WHEN balance_usd <   100000       THEN '10k-100k'
            WHEN balance_usd <  1000000       THEN '100k-1M'
            ELSE                                   '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance_usd IS NOT NULL
      AND balance_usd > 0
),

bucketed_native AS (
    SELECT
        date,
        reserve_address,
        symbol,
        user_address,
        balance,
        balance_usd,
        'native' AS cohort_unit,
        CASE
            WHEN balance <     0.01       THEN '0-0.01'
            WHEN balance <      0.1       THEN '0.01-0.1'
            WHEN balance <        1       THEN '0.1-1'
            WHEN balance <       10       THEN '1-10'
            WHEN balance <      100       THEN '10-100'
            WHEN balance <     1000       THEN '100-1k'
            WHEN balance <    10000       THEN '1k-10k'
            WHEN balance <   100000       THEN '10k-100k'
            WHEN balance <  1000000       THEN '100k-1M'
            ELSE                                '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance > 0
),

bucketed AS (
    SELECT date, reserve_address, symbol, user_address, balance, balance_usd, cohort_unit, balance_bucket
    FROM bucketed_usd
    UNION ALL
    SELECT date, reserve_address, symbol, user_address, balance, balance_usd, cohort_unit, balance_bucket
    FROM bucketed_native
),

agg AS (
    SELECT
        date,
        reserve_address,
        symbol,
        cohort_unit,
        balance_bucket,
        countDistinct(user_address) AS holders_in_bucket,
        sum(balance) AS value_native_in_bucket,
        sum(balance_usd) AS value_usd_in_bucket
    FROM bucketed
    GROUP BY
        date,
        reserve_address,
        symbol,
        cohort_unit,
        balance_bucket
)

SELECT
    date,
    reserve_address,
    symbol,
    cohort_unit,
    balance_bucket,
    holders_in_bucket,
    value_native_in_bucket,
    value_usd_in_bucket
FROM agg
WHERE date < today()
