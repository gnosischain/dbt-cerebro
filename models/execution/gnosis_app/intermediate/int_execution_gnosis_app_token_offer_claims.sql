{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
    unique_key='(transaction_hash, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    tags=['production','execution','gnosis_app','token_offers','claims']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{% set entrypoint = '0000000071727de22e5e9d8baf0edac6f37da032' %}

WITH ga_users AS (
    SELECT address FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

relayer_addrs AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM {{ ref('gnosis_app_relayers') }}
    WHERE is_active = 1
),

offers AS (
    SELECT
        offer_address,
        cycle_address,
        offer_token_address,
        offer_token_symbol,
        offer_token_decimals,
        token_price_in_crc
    FROM {{ ref('int_execution_gnosis_app_token_offers') }}
),

claim_events AS (
    SELECT
        e.block_number,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        lower(e.decoded_params['account'])         AS account,
        lower(e.decoded_params['offer'])           AS offer_address,
        toUInt256OrNull(e.decoded_params['received']) AS received_raw,
        toUInt256OrNull(e.decoded_params['spent'])    AS spent_raw
    FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }} e
    WHERE e.event_name = 'OfferClaimed'
      AND e.block_timestamp >= toDateTime('2025-11-12')
      {% if start_month and end_month %}
        AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('e.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

cometh_txs AS (
    SELECT
        transaction_hash,
        from_address AS relayer_address
    FROM {{ source('execution','transactions') }} tx
    WHERE tx.to_address = '{{ entrypoint }}'
      AND lower(tx.from_address) IN (SELECT addr FROM relayer_addrs)
      AND tx.block_timestamp >= toDateTime('2025-11-12')
      {% if start_month and end_month %}
        AND toStartOfMonth(tx.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(tx.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('tx.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

prices AS (
    SELECT date, symbol, price
    FROM {{ ref('int_execution_token_prices_daily') }}
)

SELECT
    e.block_number                                       AS block_number,
    e.block_timestamp                                    AS block_timestamp,
    concat('0x', e.transaction_hash)                     AS transaction_hash,
    e.log_index                                          AS log_index,
    e.account                                            AS ga_user,
    e.offer_address                                      AS offer_address,
    o.cycle_address                                      AS cycle_address,
    o.offer_token_address                                AS offer_token_address,
    o.offer_token_symbol                                 AS offer_token_symbol,
    e.received_raw                                       AS amount_received_raw,
    toFloat64(e.received_raw) / pow(10, coalesce(o.offer_token_decimals, 18))
                                                         AS amount_received,
    e.spent_raw                                          AS amount_spent_crc_raw,
    -- CRC v2 uses 1e18 base units.
    toFloat64(e.spent_raw) / 1e18                        AS amount_spent_crc,
    -- USD priced on the received side only (CRC has no reliable USD feed yet).
    (toFloat64(e.received_raw) / pow(10, coalesce(o.offer_token_decimals, 18)))
        * coalesce(p.price, 0)                           AS amount_received_usd,
    o.token_price_in_crc                                 AS offer_price_in_crc,
    concat('0x', ct.relayer_address)                     AS relayer_address
FROM claim_events e
INNER JOIN cometh_txs ct
    ON ct.transaction_hash = e.transaction_hash
INNER JOIN ga_users u
    ON u.address = e.account
LEFT JOIN offers o
    ON o.offer_address = e.offer_address
LEFT JOIN prices p
    ON p.date = toDate(e.block_timestamp)
   AND p.symbol = upper(o.offer_token_symbol)
