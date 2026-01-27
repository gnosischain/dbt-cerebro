{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, balance_bucket)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, balance_bucket)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','tokens','balance_cohorts_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

balances_filtered AS (
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
      AND b.balance_usd IS NOT NULL
      AND b.balance_usd > 0
      AND lower(b.address) != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
),

bucketed AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        balance,
        balance_usd,
        CASE
            WHEN balance_usd <       10       THEN '0-10'
            WHEN balance_usd <      100       THEN '10-100'
            WHEN balance_usd <     1000       THEN '100-1k'
            WHEN balance_usd <    10000       THEN '1k-10k'
            WHEN balance_usd <   100000       THEN '10k-100k'
            WHEN balance_usd <  1000000       THEN '100k-1M'
            ELSE                                  '1M+'
        END AS balance_bucket
    FROM balances_filtered
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
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
        balance_bucket
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    holders_in_bucket,
    value_native_in_bucket,
    value_usd_in_bucket
FROM agg
WHERE date < today()
ORDER BY date, token_address, balance_bucket

