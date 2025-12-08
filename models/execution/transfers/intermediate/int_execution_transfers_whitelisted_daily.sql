{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address, "from", "to")',
        unique_key='(date, token_address, "from", "to")',
        partition_by='toStartOfMonth(date)',
        settings={ 'allow_nullable_key': 1 },
        tags=['production', 'execution', 'transfers', 'erc20', 'whitelisted', 'daily']
    )
}}

{% set start_month      = var('start_month', none) %}
{% set end_month        = var('end_month', none) %}

WITH tokens AS (
    SELECT
        lower(address)                       AS token_address,
        decimals,
        symbol,
        date_start,
        date_end
    FROM {{ ref('tokens_whitelist') }}
    WHERE symbol != 'WxDAI'
),

raw_whitelisted_transfers AS (
    SELECT
        toDate(l.block_timestamp) AS date,
        t.token_address,
        t.symbol,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS "from",
        lower(concat('0x', substring(l.topic2, 25, 40))) AS "to",
        reinterpretAsInt256(
                reverse(unhex(l.data))
            ) AS value_raw
    FROM {{ ref('stg_execution__logs') }} AS l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address
    WHERE
        l.topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND l.block_timestamp < today()
        {% if start_month and end_month %}
          AND toStartOfMonth(l.block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(l.block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
        {% endif %}
),

transfers_whitelisted_daily AS (
    SELECT
        date,
        token_address,
        any(symbol)       AS symbol,
        "from",
        "to",
        sum(value_raw) AS amount_raw,
        count() AS transfer_count
    FROM raw_whitelisted_transfers
    GROUP BY
        date, token_address, "from", "to"
),


wxdai_logs AS (
    SELECT
        toDate(block_timestamp) AS date
        ,'0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' AS token_address
        ,'WxDAI' AS symbol
        ,decoded_params
        ,event_name
    FROM {{ ref('contracts_wxdai_events') }} 
    WHERE (event_name = 'Withdrawal' OR event_name = 'Transfer' OR event_name = 'Deposit')
        AND block_timestamp < today()
        {% if start_month and end_month %}
          AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
        {% endif %}
),

deposits_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,'0x0000000000000000000000000000000000000000' AS "from"
        ,decoded_params['dst'] AS "to"
        ,toInt256(decoded_params['wad']) AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Deposit'
),

withdrawals_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,decoded_params['src'] AS "from"
        ,'0x0000000000000000000000000000000000000000' AS "to"
        ,toInt256(decoded_params['wad'])AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Withdrawal'
),

transfers_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,decoded_params['src'] AS "from"
        ,decoded_params['dst']  AS "to"
        ,toInt256(decoded_params['wad'])AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Transfer'
),

transfers_wxdai_daily AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,"from"
        ,"to"
        ,SUM(value_raw) AS value_raw
        ,count() AS transfer_count
    FROM (
        SELECT * FROM deposits_wxdai
        UNION ALL 
        SELECT * FROM withdrawals_wxdai
        UNION ALL 
        SELECT * FROM transfers_wxdai
    )
    GROUP BY 1, 2, 3, 4, 5
),

transfers_daily AS (
    SELECT * FROM transfers_whitelisted_daily
    UNION ALL
    SELECT * FROM transfers_wxdai_daily
)


SELECT
    date,
    token_address,
    symbol,
    "from",
    "to",
    amount_raw,
    transfer_count
FROM transfers_daily 


