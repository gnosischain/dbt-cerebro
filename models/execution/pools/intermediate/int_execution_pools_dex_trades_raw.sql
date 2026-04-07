{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET join_use_nulls = 0",
            "SET allow_experimental_json_type = 1"
        ],
        tags=['production', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

all_swaps AS (
    SELECT * FROM {{ ref('stg_pools__dex_trades_uniswap_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_trades_swapr_v3') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_trades_balancer_v2') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_pools__dex_trades_balancer_v3') }}
)

SELECT
    s.block_number,
    s.block_timestamp,
    s.transaction_hash,
    s.log_index,
    s.protocol,
    s.pool_address,
    s.token_bought_address,
    tb.token                                                                         AS token_bought_symbol,
    s.amount_bought_raw,
    s.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))           AS amount_bought,
    s.token_sold_address,
    ts.token                                                                         AS token_sold_symbol,
    s.amount_sold_raw,
    s.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))             AS amount_sold,
    s.taker
FROM all_swaps s
LEFT JOIN {{ ref('stg_pools__tokens_meta') }} tb
    ON  tb.token_address = s.token_bought_address
    AND toDate(s.block_timestamp) >= toDate(tb.date_start)
LEFT JOIN {{ ref('stg_pools__tokens_meta') }} ts
    ON  ts.token_address = s.token_sold_address
    AND toDate(s.block_timestamp) >= toDate(ts.date_start)
WHERE s.amount_bought_raw > 0
  AND s.amount_sold_raw   > 0
  {% if not (start_month and end_month) %}
    {{ apply_monthly_incremental_filter('s.block_timestamp', 'block_timestamp', 'true') }}
  {% endif %}
