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
),

raw_whitelisted_logs AS (
    SELECT
        toDate(l.block_timestamp) AS date,
        l.block_timestamp,
        t.token_address,
        t.symbol,
        t.decimals,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS "from",
        lower(concat('0x', substring(l.topic2, 25, 40))) AS "to",
        toString(
            reinterpretAsUInt256(
                reverse(unhex(l.data))
            )
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

amounts_daily AS (
    SELECT
        date,
        token_address,
        any(symbol)       AS symbol,
        "from",
        "to",
        sum(
          toFloat64OrZero(value_raw) / pow(10, decimals)
        ) AS amount_token,
        count() AS transfer_count
    FROM raw_whitelisted_logs
    GROUP BY
        date, token_address, "from", "to"
),

priced AS (
    SELECT
        a.date,
        a.token_address,
        a.symbol,
        a."from",
        a."to",
        a.amount_token,
        a.transfer_count,
        p.price
    FROM amounts_daily a
    LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
      ON p.date = a.date
     AND upper(p.symbol) = upper(a.symbol)
)

SELECT
    date,
    token_address,
    symbol,
    "from",
    "to",
    amount_token AS amount,
    amount_token * price AS amount_usd,
    transfer_count
FROM priced