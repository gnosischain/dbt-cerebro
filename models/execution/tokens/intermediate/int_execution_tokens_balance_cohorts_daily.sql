{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, cohort_unit, balance_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, cohort_unit, balance_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','balance_cohorts_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

balances_base AS (
    SELECT
        b.date,
        lower(b.token_address) AS token_address,
        b.symbol AS symbol,
        b.token_class,
        lower(b.address) AS address,
        b.balance,
        b.balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
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
        token_address,
        symbol,
        token_class,
        address,
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
            ELSE                                  '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance_usd IS NOT NULL
      AND balance_usd > 0
),

bucketed_native AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
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
            ELSE                                  '1M+'
        END AS balance_bucket
    FROM balances_base
    WHERE balance > 0
),

bucketed AS (
    SELECT * FROM bucketed_usd
    UNION ALL
    SELECT * FROM bucketed_native
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        cohort_unit,
        balance_bucket,
        countDistinct(address) AS holders_in_bucket,
        sum(balance) AS value_native_in_bucket,
        sum(balance_usd) AS value_usd_in_bucket
    FROM bucketed
    GROUP BY
        date,
        token_address,
        symbol,
        token_class,
        cohort_unit,
        balance_bucket
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    cohort_unit,
    balance_bucket,
    holders_in_bucket,
    value_native_in_bucket,
    value_usd_in_bucket
FROM agg
WHERE date < today()
ORDER BY date, token_address, cohort_unit, balance_bucket
