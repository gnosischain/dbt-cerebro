{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={ 'allow_nullable_key': 1 },
        tags=['dev', 'execution', 'transfers', 'erc20', 'whitelisted']
    )
}}

{% set start_month      = var('start_month', none) %}
{% set end_month        = var('end_month', none) %}
{% set day_index_start  = var('day_index_start', none) %}
{% set day_index_end    = var('day_index_end', none) %}

WITH tokens AS (
    SELECT
        lower(address)                           AS token_address,      
        lower(replaceAll(address, '0x', ''))     AS token_address_raw,  
        decimals,
        symbol,
        upper(symbol)                            AS symbol_upper,       
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
        concat('0x', lower(replaceAll(l.transaction_hash, '0x', ''))) AS transaction_hash,
        t.token_address,
        t.symbol,
        t.symbol_upper,
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
       AND toDate(l.block_timestamp) >= t.date_start
       AND (t.date_end IS NULL OR toDate(l.block_timestamp) < t.date_end)
    WHERE
        lower(replaceAll(l.topic0, '0x', '')) =
          'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND l.block_timestamp < today()
        {% if start_month and end_month %}
          AND toStartOfMonth(l.block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(l.block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', 'true') }}
        {% endif %}
        {% if day_index_start and day_index_end %}
          AND toDayOfMonth(l.block_timestamp) >= {{ day_index_start }}
          AND toDayOfMonth(l.block_timestamp) <= {{ day_index_end }}
        {% endif %}
),

prices_rwa AS (
    SELECT
        toDate(date)             AS date,
        upper(bticker)           AS symbol_upper,
        price
    FROM {{ ref('api_execution_rwa_backedfi_prices_daily') }}
),

prices_dune_raw AS (
    SELECT
        date,
        upper(symbol)            AS symbol_upper,
        price
    FROM {{ ref('stg_crawlers_data__dune_prices') }}
),

prices_dune AS (
    SELECT date, symbol_upper, price
    FROM prices_dune_raw
    UNION ALL
    SELECT date, 'WXDAI' AS symbol_upper, price
    FROM prices_dune_raw
    WHERE symbol_upper = 'XDAI'
),

prices AS (
    SELECT date, symbol_upper, price FROM prices_rwa
    UNION ALL
    SELECT date, symbol_upper, price FROM prices_dune
),

enriched AS (
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
        r.symbol_upper,
        r.decimals,
        r.value_raw,
        r.date_start,
        r.date_end,
        toFloat64OrZero(r.value_raw) / pow(10, r.decimals) AS amount,
        coalesce(
            p.price,
            case
              when r.symbol_upper IN ('USDC','USDC.E','USDT') then 1.0
              when r.symbol_upper = 'WXDAI'                   then 1.0   
              else null
            end
        ) AS price
    FROM raw_whitelisted_logs r
    LEFT JOIN prices p
      ON p.date = toDate(r.block_timestamp)
     AND p.symbol_upper = r.symbol_upper
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
    amount,
    price,
    amount * price AS amount_usd,
    value_raw
FROM enriched