{% set daily_rate_eure  = 0.0000096    %}  {# 0.351% APY #}
{% set daily_rate_usdce = 0.0000096    %}  {# 0.351% APY #}
{% set daily_rate_brla  = 0.0000561349 %}  {# 2.07%  APY #}
{% set daily_rate_zchf  = 0.0000136646 %}  {# 0.5%   APY #}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(date, symbol, user)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    pre_hook=[
      "SET join_algorithm = 'grace_hash'",
      "SET max_bytes_in_join = 500000000",
      "SET max_bytes_before_external_group_by = 2000000000",
      "SET max_bytes_before_external_sort = 2000000000"
    ],
    post_hook=[
      "SET join_algorithm = 'default'",
      "SET max_bytes_in_join = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production','revenue','revenue_holdings','refill_append']
  )
}}

-- Users are EOAs and Safes only. Protocol/token contracts (pools, vaults,
-- aTokens, routers) hold balances but are not fee-paying users; aToken
-- contracts in particular would double count the Aave look-through branch.
-- The exclusion runs as a single LEFT ANTI JOIN after the union (NOT a
-- NOT IN subquery: a 5.5M-address IN-set materializes via
-- CreatingSetsTransform which cannot spill to disk and OOMs the 10.8 GiB
-- instance under nightly load; joins spill with grace_hash).
WITH non_users AS (
    SELECT address FROM {{ ref('int_execution_accounts_non_user_contracts') }}
),

base AS (
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

base_users AS (
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN non_users nu ON b.user = nu.address
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
    FROM base_users
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
