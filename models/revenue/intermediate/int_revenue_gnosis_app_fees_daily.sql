{% set metri_address = '0x97fd8f7829a019946329f6d2e763a72741047518' %}
{% set start_date    = '2025-11-12' %}
{% set start_month   = var('start_month', none) %}
{% set end_month     = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(date, user)',
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
    tags=['production', 'revenue', 'revenue_gnosis_app']
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

metri_transfers_raw AS (
    SELECT
        toDate(block_timestamp)      AS date,
        from_address                 AS user,
        token_address                AS avatar,
        toFloat64(amount_raw) / 1e18 AS fee_native
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE to_address = '{{ metri_address }}'
      AND block_timestamp >= toDateTime('{{ start_date }}')
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
      {% endif %}
),

metri_transfers AS (
    SELECT m.*
    FROM metri_transfers_raw m
    LEFT ANTI JOIN non_users nu ON lower(m.user) = nu.address
),

-- Per-token daily price: collapse multiple pools to one price per (date, avatar)
token_prices AS (
    SELECT
        date,
        avatar,
        median(price_avg_usd) AS price
    FROM {{ ref('fct_execution_circles_v2_crc20_prices_daily') }}
    WHERE price_avg_usd IS NOT NULL
    GROUP BY date, avatar
),

-- Fallback: daily median across all tokens with a price that day
median_prices AS (
    SELECT
        date,
        median(price_avg_usd) AS price_fallback
    FROM {{ ref('fct_execution_circles_v2_crc20_prices_daily') }}
    WHERE price_avg_usd IS NOT NULL
    GROUP BY date
)

SELECT
    t.date                                                       AS date,
    t.user                                                       AS user,
    'CRC'                                                        AS symbol,
    sum(t.fee_native)                                            AS fees_native,
    sum(t.fee_native * COALESCE(tp.price, mp.price_fallback))    AS fees
FROM metri_transfers t
LEFT JOIN token_prices tp
    ON tp.date   = t.date
   AND tp.avatar = t.avatar
LEFT JOIN median_prices mp
    ON mp.date = t.date
GROUP BY t.date, t.user
