{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

swaps AS (
    SELECT
        s.*,
        tx.tx_from,
        tx.tx_to
    FROM (
        SELECT *
        FROM {{ ref('int_execution_pools_dex_trades_raw') }}
        {% if start_month and end_month %}
        WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
        {% endif %}
    ) s
    LEFT JOIN (
        SELECT transaction_hash, tx_from, tx_to
        FROM {{ ref('int_execution_pools_dex_trades_tx_context') }}
        {% if start_month and end_month %}
        WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
        {% endif %}
    ) tx ON tx.transaction_hash = s.transaction_hash
),

with_bought_price AS (
    SELECT
        s.*,
        pb.price_usd AS token_bought_price_usd
    FROM swaps s
    ASOF LEFT JOIN (
        SELECT token, date, price_usd
        FROM {{ ref('stg_pools__token_prices_daily') }}
        {% if start_month and end_month %}
        WHERE date BETWEEN toDate('{{ start_month }}') - INTERVAL 30 DAY
                       AND toDate('{{ end_month }}') + INTERVAL 32 DAY
        {% endif %}
        ORDER BY token, date
    ) pb
        ON  pb.token                  = s.token_bought_symbol
        AND toDate(s.block_timestamp) >= pb.date
),

with_sold_price AS (
    SELECT
        s.*,
        ps.price_usd AS token_sold_price_usd
    FROM with_bought_price s
    ASOF LEFT JOIN (
        SELECT token, date, price_usd
        FROM {{ ref('stg_pools__token_prices_daily') }}
        {% if start_month and end_month %}
        WHERE date BETWEEN toDate('{{ start_month }}') - INTERVAL 30 DAY
                       AND toDate('{{ end_month }}') + INTERVAL 32 DAY
        {% endif %}
        ORDER BY token, date
    ) ps
        ON  ps.token                  = s.token_sold_symbol
        AND toDate(s.block_timestamp) >= ps.date
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    token_bought_address,
    token_bought_symbol,
    amount_bought_raw,
    amount_bought,
    token_sold_address,
    token_sold_symbol,
    amount_sold_raw,
    amount_sold,
    COALESCE(
        amount_bought * token_bought_price_usd,
        amount_sold   * token_sold_price_usd
    )                   AS amount_usd,
    coalesce(taker, tx_from) AS taker,
    tx_from,
    tx_to
FROM with_sold_price
