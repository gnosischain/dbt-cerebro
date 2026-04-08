{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index, token_address)',
        unique_key='(block_timestamp, transaction_hash, log_index, token_address)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET allow_experimental_json_type = 1"
        ],
        tags=['production', 'execution', 'pools', 'liquidity', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

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

events_base AS (
    SELECT
        l.block_number,
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
        l.amount_raw / POWER(10, if(tm.decimals > 0, tm.decimals, 18))         AS amount,
        l.tick_lower,
        l.tick_upper,
        l.liquidity_delta
    FROM all_liquidity l
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tm
        ON  tm.token_address          = l.token_address
        AND toDate(l.block_timestamp) >= toDate(tm.date_start)
    WHERE l.amount_raw > 0
      {% if not (start_month and end_month) %}
        {{ apply_monthly_incremental_filter('l.block_timestamp', 'block_timestamp', 'true') }}
      {% endif %}
),

tx_context AS (
    SELECT DISTINCT transaction_hash, from_address, to_address
    FROM {{ source('execution', 'transactions') }}
    WHERE transaction_hash IN (SELECT DISTINCT transaction_hash FROM events_base)
      {% if start_month and end_month %}
      AND block_timestamp >= toDate('{{ start_month }}') - INTERVAL 1 DAY
      AND block_timestamp <= toDate('{{ end_month }}') + INTERVAL 32 DAY
      {% elif is_incremental() %}
      AND block_timestamp >= (
          SELECT addDays(max(toDate(block_timestamp)), -3)
          FROM {{ this }}
      )
      {% endif %}
),

with_meta AS (
    SELECT
        e.*,
        lower(tx.from_address) AS tx_from,
        lower(tx.to_address)   AS tx_to
    FROM events_base e
    LEFT JOIN tx_context tx ON tx.transaction_hash = e.transaction_hash
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
    block_number,
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
    amount * token_price_usd AS amount_usd,
    tick_lower,
    tick_upper,
    liquidity_delta,
    tx_from,
    tx_to
FROM with_price
ORDER BY block_timestamp, transaction_hash, log_index, token_address
