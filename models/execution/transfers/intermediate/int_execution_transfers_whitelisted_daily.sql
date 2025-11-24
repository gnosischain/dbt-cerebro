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
{% set day_index_start  = var('day_index_start', none) %}
{% set day_index_end    = var('day_index_end', none) %}

WITH tokens AS (
    SELECT
        lower(address)                       AS token_address,
        lower(replaceAll(address, '0x', '')) AS token_address_raw,
        decimals,
        symbol,
        upper(symbol)                        AS symbol_upper,
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
        t.symbol_upper,
        t.decimals,
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
        lower(replaceAll(l.topic0, '0x', '')) =
          'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND l.block_timestamp < today()

        {% if start_month and end_month %}
          AND toStartOfMonth(l.block_timestamp) >= toDate('{{ start_month }}')
          AND toStartOfMonth(l.block_timestamp) <= toDate('{{ end_month }}')
        {% else %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
        {% endif %}

        {% if day_index_start and day_index_end %}
          AND toDayOfMonth(l.block_timestamp) >= {{ day_index_start }}
          AND toDayOfMonth(l.block_timestamp) <= {{ day_index_end }}
        {% endif %}
),

amounts_daily AS (
    SELECT
        date,
        token_address,
        any(symbol)       AS symbol,
        any(symbol_upper) AS symbol_upper,
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
     AND p.symbol = a.symbol_upper
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
ORDER BY date, token_address, "from", "to"