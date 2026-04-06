{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index, token_address)',
        unique_key='(block_timestamp, transaction_hash, log_index, token_address)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET join_use_nulls = 0",
            "SET allow_experimental_json_type = 1"
        ],
        tags=['production', 'execution', 'pools', 'liquidity', 'intermediate']
    )
}}

{#-
  Individual LP add/remove events across all protocols on Gnosis Chain.
  One row per (event, token) — deliberately not pivoted to wide format
  because Balancer pools can hold 2–8 tokens, making a fixed-width
  schema incorrect. Consumers aggregate per event when needed.

  Each protocol's event decoding lives in its own staging model
  (stg_pools__dex_liquidity_<protocol>) for easy addition of new protocols.
  This model unions them, enriches with token metadata, and adds USD prices.

  Protocols:
    - Uniswap V3  : stg_pools__dex_liquidity_uniswap_v3
    - Swapr V3    : stg_pools__dex_liquidity_swapr_v3
    - Balancer V2 : stg_pools__dex_liquidity_balancer_v2
    - Balancer V3 : stg_pools__dex_liquidity_balancer_v3

  event_type values: 'mint' | 'burn'
  amount_raw is always positive; event_type conveys direction.

  Batching:
    When called with start_month/end_month vars (e.g. via the full-refresh script),
    each protocol model filters to that month window. Otherwise falls back to the
    incremental filter based on max(block_timestamp) in the existing table.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

all_liquidity AS (
    SELECT * FROM {{ ref('stg_pools__dex_liquidity_uniswap_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_liquidity_swapr_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_liquidity_balancer_v2') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_liquidity_balancer_v3') }}
),

with_meta AS (
    SELECT
        l.block_timestamp,
        l.transaction_hash,
        l.log_index,
        l.protocol,
        l.pool_address,
        l.provider,
        l.event_type,
        l.token_address,
        tm.token                                                                 AS token_symbol,
        l.amount_raw,
        l.amount_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))         AS amount
    FROM all_liquidity l
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
        ON  tm.token_address          = l.token_address
        AND toDate(l.block_timestamp) >= toDate(tm.date_start)
    WHERE l.amount_raw > 0
      {% if not (start_month and end_month) %}
        {{ apply_monthly_incremental_filter('l.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

with_price AS (
    SELECT
        s.*,
        pr.price_usd AS token_price_usd
    FROM with_meta s
    ASOF LEFT JOIN (
        SELECT token, date, price_usd
        FROM {{ ref('stg_pools__token_prices_daily') }}
        ORDER BY token, date
    ) pr
        ON  pr.token                  = s.token_symbol
        AND toDate(s.block_timestamp) >= pr.date
)

SELECT
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    provider,
    event_type,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount * token_price_usd AS amount_usd
FROM with_price
ORDER BY block_timestamp, transaction_hash, log_index, token_address
