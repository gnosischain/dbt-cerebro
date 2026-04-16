{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(gateway_address)',
    unique_key='gateway_address',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','marketplace']
  )
}}

{# Description in schema.yml — see int_execution_gnosis_app_marketplace_offers #}

{#
  One row per non-excluded PaymentGateway.

  Sources:
    1. contracts_circles_v2_PaymentGatewayFactory_calls (traces-decoded
       via decode_calls auto-detect) — carries the `name` string input.
    2. contracts_circles_v2_PaymentGatewayFactory_events.GatewayCreated
       — carries the resulting `gateway` address and `owner`.

  Joined on transaction_hash since createGateway emits GatewayCreated in
  the same tx.

  Blocklist: seeds/gnosis_app_marketplace_offers_excluded.csv. A row
  matches the blocklist if offer_name (case-insensitive) OR
  gateway_address matches any row there. Empty string means "ignore
  this column for this blocklist row".
#}

WITH

create_calls AS (
    SELECT
        transaction_hash,
        block_timestamp,
        decoded_input['name']                    AS offer_name
    FROM {{ ref('contracts_circles_v2_PaymentGatewayFactory_calls') }}
    WHERE function_name = 'createGateway'
),

gateway_events AS (
    SELECT
        transaction_hash,
        lower(decoded_params['gateway'])         AS gateway_address,
        lower(decoded_params['owner'])           AS creator
    FROM {{ ref('contracts_circles_v2_PaymentGatewayFactory_events') }}
    WHERE event_name = 'GatewayCreated'
),

excluded AS (
    SELECT
        lower(coalesce(offer_name, ''))          AS offer_name_lower,
        lower(coalesce(gateway_address, ''))     AS gateway_address_lower
    FROM {{ ref('gnosis_app_marketplace_offers_excluded') }}
)

SELECT
    e.gateway_address                            AS gateway_address,
    c.offer_name                                 AS offer_name,
    c.block_timestamp                            AS created_at,
    e.creator                                    AS creator
FROM gateway_events e
INNER JOIN create_calls c
    ON c.transaction_hash = e.transaction_hash
WHERE c.offer_name IS NOT NULL
  AND c.offer_name != ''
  AND lower(c.offer_name) NOT IN (
        SELECT offer_name_lower FROM excluded WHERE offer_name_lower != ''
      )
  AND e.gateway_address NOT IN (
        SELECT gateway_address_lower FROM excluded WHERE gateway_address_lower != ''
      )
