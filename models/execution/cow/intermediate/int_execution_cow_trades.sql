{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'cow', 'trades', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

swaps AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash                                                           AS transaction_hash,
        t.log_index,
        t.protocol,
        t.pool_address,
        t.token_bought_address,
        tb.token                                                                     AS token_bought_symbol,
        t.amount_bought_raw,
        t.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))       AS amount_bought,
        t.token_sold_address,
        ts.token                                                                     AS token_sold_symbol,
        t.amount_sold_raw,
        t.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))         AS amount_sold,
        t.fee_amount_raw,
        t.fee_amount_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))          AS fee_amount,
        t.taker,
        t.order_uid,
        st.solver
    FROM (
        SELECT *
        FROM {{ ref('stg_cow__trades') }}
        {% if start_month and end_month %}
        WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
        {% endif %}
    ) t
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tb
        ON  tb.token_address = t.token_bought_address
        AND toDate(t.block_timestamp) >= toDate(tb.date_start)
    LEFT JOIN {{ ref('stg_pools__tokens_meta') }} ts
        ON  ts.token_address = t.token_sold_address
        AND toDate(t.block_timestamp) >= toDate(ts.date_start)
    LEFT JOIN (
        SELECT transaction_hash, solver
        FROM {{ ref('stg_cow__settlements') }}
        {% if start_month and end_month %}
        WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
        {% elif is_incremental() %}
        WHERE block_timestamp >= (SELECT addDays(max(toDate(block_timestamp)), -3) FROM {{ this }})
        {% endif %}
    ) st ON st.transaction_hash = t.transaction_hash
    WHERE t.amount_bought_raw > 0
      AND t.amount_sold_raw   > 0
),

with_bought_price AS (
    SELECT
        s.*,
        pb.price AS token_bought_price_usd
    FROM swaps s
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM {{ ref('int_execution_token_prices_daily') }}
        {% if start_month and end_month %}
        WHERE date BETWEEN toDate('{{ start_month }}') - INTERVAL 30 DAY
                       AND toDate('{{ end_month }}') + INTERVAL 32 DAY
        {% elif is_incremental() %}
        WHERE date >= (SELECT addDays(max(toDate(block_timestamp)), -30) FROM {{ this }})
        {% endif %}
        ORDER BY symbol, date
    ) pb
        ON  pb.symbol                 = s.token_bought_symbol
        AND toDate(s.block_timestamp) >= pb.date
),

with_sold_price AS (
    SELECT
        s.*,
        ps.price AS token_sold_price_usd
    FROM with_bought_price s
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM {{ ref('int_execution_token_prices_daily') }}
        {% if start_month and end_month %}
        WHERE date BETWEEN toDate('{{ start_month }}') - INTERVAL 30 DAY
                       AND toDate('{{ end_month }}') + INTERVAL 32 DAY
        {% elif is_incremental() %}
        WHERE date >= (SELECT addDays(max(toDate(block_timestamp)), -30) FROM {{ this }})
        {% endif %}
        ORDER BY symbol, date
    ) ps
        ON  ps.symbol                 = s.token_sold_symbol
        AND toDate(s.block_timestamp) >= ps.date
)

SELECT
    block_number,
    block_timestamp,
    concat('0x', transaction_hash)                                                   AS transaction_hash,
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
    fee_amount_raw,
    fee_amount,
    COALESCE(
        amount_bought * token_bought_price_usd,
        amount_sold   * token_sold_price_usd
    )                                                                                AS amount_usd,
    taker,
    order_uid,
    solver
FROM with_sold_price
