{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
    unique_key='(transaction_hash, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    tags=['production','execution','gnosis_app','marketplace']
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
    SELECT gateway_address, offer_name
    FROM {{ ref('int_execution_gnosis_app_marketplace_offers') }}
),

payment_events AS (
    SELECT
        e.block_number,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        lower(e.decoded_params['payer'])            AS payer,
        lower(e.decoded_params['payee'])            AS payee,
        lower(e.decoded_params['gateway'])          AS gateway_address,
        toUInt256OrNull(e.decoded_params['tokenId']) AS token_id,
        toUInt256OrNull(e.decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_circles_v2_PaymentGatewayFactory_events') }} e
    WHERE e.event_name = 'PaymentReceived'
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
)

SELECT
    p.block_number                              AS block_number,
    p.block_timestamp                           AS block_timestamp,
    concat('0x', p.transaction_hash)            AS transaction_hash,
    p.log_index                                 AS log_index,
    p.payer                                     AS payer,
    p.payee                                     AS payee,
    p.gateway_address                           AS gateway_address,
    o.offer_name                                AS offer_name,
    p.token_id                                  AS token_id,
    p.amount_raw                                AS amount_raw,
    -- CRC v2 is 1e18 scaled
    toFloat64(p.amount_raw) / 1e18              AS amount,
    concat('0x', ct.relayer_address)            AS relayer_address
FROM payment_events p
INNER JOIN cometh_txs ct
    ON ct.transaction_hash = p.transaction_hash
INNER JOIN offers o
    ON o.gateway_address = p.gateway_address
INNER JOIN ga_users u
    ON u.address = p.payer
