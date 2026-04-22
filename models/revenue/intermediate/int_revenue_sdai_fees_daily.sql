{% set dao_share_pct = 0.1 %}  {# 10% of full rate accrues to Gnosis DAO #}

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
    tags=['production','revenue','revenue_sdai']
  )
}}

WITH base AS (
    -- Native sDAI balances.
    SELECT date, address AS user, balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }}
    WHERE date < today()
      AND balance_usd > 0
      AND address IS NOT NULL
      AND symbol = 'sDAI'
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
      {% endif %}

    UNION ALL

    -- sDAI held in Aave V3 (aGnosDAI). SparkLend excluded.
    SELECT date, user_address AS user, balance_usd
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
    WHERE date < today()
      AND balance_usd > 0
      AND user_address IS NOT NULL
      AND protocol = 'Aave V3'
      AND symbol = 'sDAI'
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
      {% endif %}
),

balances AS (
    SELECT date, user, sum(balance_usd) AS balance_usd_sum
    FROM base
    GROUP BY date, user
),

rates AS (
    SELECT date, rate
    FROM {{ ref('int_yields_sdai_rate_daily') }}
    WHERE rate IS NOT NULL
),

joined AS (
    SELECT
        b.date,
        b.user,
        b.balance_usd_sum,
        r.rate,
        b.balance_usd_sum * r.rate * toFloat64({{ dao_share_pct }}) AS fees_raw
    FROM balances b
    INNER JOIN rates r USING (date)
)

SELECT
    date,
    user,
    'sDAI' AS symbol,
    round(fees_raw, 8)        AS fees,
    round(balance_usd_sum, 6) AS balance_usd_total
FROM joined
