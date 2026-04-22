{% set daily_rate_eure  = 0.0000096    %}  {# 0.351% APY #}
{% set daily_rate_usdce = 0.0000096    %}  {# 0.351% APY #}
{% set daily_rate_brla  = 0.0000561349 %}  {# 2.07%  APY #}
{% set daily_rate_zchf  = 0.0000136646 %}  {# 0.5%   APY #}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, symbol, user)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, symbol, user)',
    settings={'allow_nullable_key': 1},
    tags=['production','revenue','revenue_holdings']
  )
}}

WITH base AS (
    -- Native ERC20 balances. svZCHF gets folded into ZCHF below.
    SELECT
        date,
        address AS user,
        multiIf(
            symbol = 'svZCHF', 'ZCHF',
            symbol
        ) AS symbol,
        balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today()
      AND balance_usd > 0
      AND address IS NOT NULL
      AND symbol IN ('EURe','USDC.e','BRLA','ZCHF','svZCHF')
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
      {% endif %}

    UNION ALL

    -- Aave V3 aToken balances (SparkLend excluded). The `symbol` column
    -- already holds the underlying reserve symbol (e.g. 'EURe' for an
    -- aGnoEURe holder), not the aToken symbol.
    SELECT
        date,
        user_address AS user,
        symbol,
        balance_usd
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
    WHERE date < today()
      AND balance_usd > 0
      AND user_address IS NOT NULL
      AND protocol = 'Aave V3'
      AND symbol IN ('EURe','USDC.e','BRLA','ZCHF')
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
      {% endif %}
),

balances AS (
    SELECT
        date,
        user,
        symbol,
        balance_usd,
        multiIf(
            symbol = 'EURe',   toFloat64({{ daily_rate_eure }}),
            symbol = 'USDC.e', toFloat64({{ daily_rate_usdce }}),
            symbol = 'BRLA',   toFloat64({{ daily_rate_brla }}),
            symbol = 'ZCHF',   toFloat64({{ daily_rate_zchf }}),
            toFloat64(0)
        ) AS daily_rate
    FROM base
)

SELECT
    date,
    user,
    symbol,
    round(sum(balance_usd * daily_rate), 8) AS fees,
    round(sum(balance_usd), 6)              AS balance_usd_total
FROM balances
WHERE daily_rate > 0
GROUP BY date, user, symbol
