{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={ 'allow_nullable_key': 1 },
        tags=['production', 'execution', 'transfers', 'erc20', 'whitelisted']
    )
}}

{% set month       = var('month', none) %}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH tokens AS (
    SELECT
        lower(address)                            AS token_address,      
        lower(replaceAll(address, '0x', ''))      AS token_address_raw,  
        decimals,
        symbol,
        date_start,                               
        date_end                                  
    FROM {{ ref('tokens_whitelist') }}
),

raw_whitelisted_logs AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.transaction_index,
        l.log_index,
        l.transaction_hash,
        t.token_address,
        t.symbol,
        t.decimals,
        t.date_start,
        t.date_end,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS "from",
        lower(concat('0x', substring(l.topic2, 25, 40))) AS "to",
        toString(
            reinterpretAsUInt256(
                reverse(unhex(replaceAll(l.data, '0x', '')))
            )
        ) AS value_raw
    FROM {{ ref('stg_execution__logs') }} AS l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address_raw
    WHERE
        l.topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        {% if var('start_month', none) and var('end_month', none) %}
          AND toStartOfMonth(l.block_timestamp) >= toDate('{{ var("start_month") }}')
          AND toStartOfMonth(l.block_timestamp) <= toDate('{{ var("end_month") }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', 'true') }}
        {% endif %}
),

filtered_active_tokens AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.transaction_index,
        r.log_index,
        r.transaction_hash,
        r."from",
        r."to",
        r.token_address,
        r.symbol,
        r.decimals,
        r.value_raw,
        r.date_start,
        r.date_end,
        CASE
            WHEN r.block_timestamp < r.date_start THEN 0
            WHEN r.date_end IS NOT NULL AND r.block_timestamp > r.date_end THEN 0
            ELSE 1
        END AS is_active_token
    FROM raw_whitelisted_logs r
)

SELECT
    block_number,
    block_timestamp,
    transaction_index,
    log_index,
    transaction_hash,
    "from",
    "to",
    token_address,
    symbol,
    decimals,
    toFloat64OrZero(value_raw) / pow(10, decimals) AS amount,
    value_raw,
    date_start,
    date_end
FROM filtered_active_tokens
WHERE is_active_token = 1