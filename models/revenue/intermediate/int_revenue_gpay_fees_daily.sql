{% set settlement_address = '0x4822521e6135cd2599199c83ea35179229a172ee' %}

{% set fee_bps_eure  = 20  %}  {# 0.20% #}
{% set fee_bps_gbpe  = 20  %}  {# 0.20% #}
{% set fee_bps_usdce = 100 %}  {# 1.00% #}

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
      "SET max_bytes_in_join = 500000000"
    ],
    post_hook=[
      "SET join_algorithm = 'default'",
      "SET max_bytes_in_join = 0"
    ],
    tags=['production','revenue','revenue_gpay','refill_append']
  )
}}

-- Users are EOAs and Safes only (see int_execution_accounts_non_user_contracts).
-- The exclusion runs as a LEFT ANTI JOIN, not a NOT IN subquery: the
-- 5.5M-address IN-set materializes via CreatingSetsTransform which cannot
-- spill to disk; joins spill with grace_hash.
WITH non_users AS (
    SELECT address FROM {{ ref('int_execution_accounts_non_user_contracts') }}

    UNION ALL

    -- Label-derived protocol/token/infra contracts (Dune labels) that the
    -- trace-built non_user_contracts set misses (e.g. Balancer Vault). Small
    -- separate model so this batched stream stays memory-safe without
    -- rebuilding the large non_user_contracts table.
    SELECT address FROM {{ ref('int_execution_accounts_label_contracts') }}
),

transfers AS (
    SELECT
        t.date,
        lower(t."from") AS user,
        t.symbol,
        multiIf(
            t.symbol = 'EURe',   toFloat64({{ fee_bps_eure  }}) / 10000.0,
            t.symbol = 'GBPe',   toFloat64({{ fee_bps_gbpe  }}) / 10000.0,
            t.symbol = 'USDC.e', toFloat64({{ fee_bps_usdce }}) / 10000.0,
            toFloat64(0)
        ) AS fee_rate,
        sum(toFloat64(t.amount_raw) / pow(10, w.decimals)) AS amount_native
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }} t
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = t.token_address
       AND t.date >= w.date_start
       AND (w.date_end IS NULL OR t.date < w.date_end)
    WHERE t.date < today()
      AND lower(t."to") = '{{ settlement_address }}'
      AND t.symbol IN ('EURe','GBPe','USDC.e')
      AND t.amount_raw IS NOT NULL
      AND t."from" IS NOT NULL
      {% if start_month and end_month %}
        AND toStartOfMonth(t.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.date', 'date', true, lookback_days=2) }}
      {% endif %}
    GROUP BY t.date, lower(t."from"), t.symbol, fee_rate
),

transfers_users AS (
    SELECT tr.*
    FROM transfers tr
    LEFT ANTI JOIN non_users nu ON tr.user = nu.address
),

prices AS (
    SELECT date, symbol, price
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE price IS NOT NULL
)

-- user is canonicalized through the June 2026 Safe migration: payments
-- made from a migrated OLD Safe are attributed to its NEW (canonical)
-- Safe so per-user fee series stay continuous. CH LEFT JOIN fills ''
-- on misses, hence the empty-string guard.
SELECT
    tr.date   AS date,
    if(c.canonical_address != '', c.canonical_address, tr.user) AS user,
    tr.symbol AS symbol,
    round(sum(tr.amount_native * tr.fee_rate), 8)           AS fees_native,
    round(sum(tr.amount_native * tr.fee_rate * p.price), 8) AS fees,
    round(sum(tr.amount_native * p.price), 6)               AS volume_usd
FROM transfers_users tr
LEFT JOIN prices p
    ON p.date = tr.date AND p.symbol = tr.symbol
LEFT JOIN {{ ref('int_execution_gpay_safe_canonical') }} c
    ON c.address = tr.user
GROUP BY tr.date, if(c.canonical_address != '', c.canonical_address, tr.user), tr.symbol
