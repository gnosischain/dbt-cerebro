{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(hour, symbol)',
    tags=['production','execution','gpay']
  )
}}

{% set logs_pre_filter %}
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND block_timestamp >= today() - 14
    AND block_timestamp < today()
{% endset %}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('int_execution_gpay_wallets') }}
),

tokens AS (
    SELECT
        lower(address) AS token_address,
        symbol,
        date_start,
        date_end
    FROM {{ ref('tokens_whitelist') }}
    WHERE symbol != 'WxDAI'
),

deduped_logs AS (
    SELECT
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        block_timestamp
    FROM (
        {{ dedup_source(
            source_ref=source('execution', 'logs'),
            partition_by='block_number, transaction_index, log_index',
            columns='address, topic1, topic2, block_timestamp',
            pre_filter=logs_pre_filter
        ) }}
    )
),

payments AS (
    SELECT
        toStartOfHour(l.block_timestamp) AS hour,
        t.symbol,
        count() AS payment_count
    FROM deduped_logs l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address
        AND l.block_timestamp >= t.date_start
        AND (t.date_end IS NULL OR l.block_timestamp < t.date_end)
    WHERE lower(concat('0x', substring(l.topic2, 25, 40))) = '0x4822521e6135cd2599199c83ea35179229a172ee'
      AND lower(concat('0x', substring(l.topic1, 25, 40))) IN (SELECT address FROM gpay_wallets)
    GROUP BY hour, t.symbol
)

SELECT
    hour,
    symbol,
    payment_count
FROM payments
ORDER BY hour, symbol
