{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(wallet_address, block_timestamp, transaction_hash, token_address, counterparty, direction)',
    partition_by='toStartOfMonth(toDate(block_timestamp))',
    unique_key='(wallet_address, block_timestamp, transaction_hash, token_address, counterparty, direction)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','activity','refill_append']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{% set logs_pre_filter %}
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND block_timestamp < today()
    AND block_timestamp >= toDate('2023-06-01')
    {% if start_month and end_month %}
      AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
    {% endif %}
{% endset %}

{% set zero_addr  = '0x0000000000000000000000000000000000000000' %}
{% set merchant   = '0x4822521e6135cd2599199c83ea35179229a172ee' %}
{% set cashback   = '0xcdf50be9061086e2ecfe6e4a1bf9164d43568eec' %}
{% set gno_token  = '0x9c58bacc331c9aa871afd802db6379a98e80cedb' %}

WITH gpay_wallets AS (
    SELECT address
    FROM {{ ref('int_execution_gpay_wallets') }}
),

tokens AS (
    SELECT
        lower(address) AS token_address,
        symbol,
        decimals,
        date_start,
        date_end
    FROM {{ ref('tokens_whitelist') }}
    --WHERE symbol IN ('EURe', 'GBPe', 'USDC.e', 'GNO')
),

deduped_logs AS (
    SELECT
        concat('0x', transaction_hash) AS transaction_hash,
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        data,
        block_timestamp
    FROM (
        {{ dedup_source(
            source_ref=source('execution', 'logs'),
            partition_by='block_number, transaction_index, log_index',
            columns='address, topic1, topic2, data, block_timestamp, transaction_hash',
            pre_filter=logs_pre_filter
        ) }}
    )
),

transfers AS (
    SELECT
        l.transaction_hash,
        l.block_timestamp,
        t.token_address,
        t.symbol,
        t.decimals,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS sender,
        lower(concat('0x', substring(l.topic2, 25, 40))) AS receiver,
        reinterpretAsInt256(reverse(unhex(l.data)))       AS value_raw
    FROM deduped_logs l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address
        AND l.block_timestamp >= t.date_start
        AND (t.date_end IS NULL OR l.block_timestamp < t.date_end)
    WHERE sender IN (SELECT address FROM gpay_wallets)
       OR receiver IN (SELECT address FROM gpay_wallets)
),

classified AS (
    SELECT
        transaction_hash,
        block_timestamp,
        token_address,
        symbol,
        decimals,
        sender,
        receiver,
        value_raw,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets)
             AND receiver = '{{ merchant }}'
            THEN 'Payment'

            WHEN receiver IN (SELECT address FROM gpay_wallets)
             AND sender = '{{ merchant }}'
            THEN 'Reversal'

            WHEN receiver IN (SELECT address FROM gpay_wallets)
             AND sender = '{{ cashback }}'
             AND token_address = '{{ gno_token }}'
            THEN 'Cashback'

            WHEN receiver IN (SELECT address FROM gpay_wallets)
             AND sender = '{{ zero_addr }}'
             AND symbol IN ('EURe', 'GBPe')
            THEN 'Fiat Top Up'

            WHEN sender IN (SELECT address FROM gpay_wallets)
             AND receiver = '{{ zero_addr }}'
             AND symbol IN ('EURe', 'GBPe')
            THEN 'Fiat Off-ramp'

            WHEN receiver IN (SELECT address FROM gpay_wallets)
            AND sender != '{{ cashback }}'
            -- AND token_address != '{{ gno_token }}'
            THEN 'Crypto Deposit'

            WHEN sender IN (SELECT address FROM gpay_wallets)
             AND receiver != '{{ merchant }}'
            -- AND token_address != '{{ gno_token }}'
            THEN 'Crypto Withdrawal'


            ELSE 'Other'
        END AS action,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets) THEN sender
            ELSE receiver
        END AS wallet_address,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets) THEN 'out'
            ELSE 'in'
        END AS direction,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets) THEN receiver
            ELSE sender
        END AS counterparty
    FROM transfers
)

SELECT
    c.transaction_hash,
    c.block_timestamp,
    toDate(c.block_timestamp) AS date,
    c.wallet_address,
    c.action,
    c.direction,
    c.symbol,
    c.token_address,
    c.counterparty,
    c.value_raw,
    c.value_raw / POWER(10, c.decimals) AS amount,
    (c.value_raw / POWER(10, c.decimals)) * coalesce(p.price, 0) AS amount_usd
FROM classified c
LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
    ON p.date = toDate(c.block_timestamp)
   AND p.symbol = c.symbol
--WHERE c.action != 'Other'
ORDER BY c.wallet_address, c.block_timestamp
