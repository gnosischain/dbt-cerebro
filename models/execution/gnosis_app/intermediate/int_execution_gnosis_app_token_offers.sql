{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cycle_address, offer_address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','token_offers'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
WITH cycles AS (
    SELECT
        contract_address                                         AS cycle_address,
        lower(decoded_params['offerToken'])                      AS offer_token_address,
        lower(decoded_params['admin'])                           AS admin,
        toDateTime(toUInt64OrNull(decoded_params['offersStart'])) AS cycle_starts_at,
        toUInt64OrNull(decoded_params['offerDuration'])          AS offer_duration_seconds
    FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }}
    WHERE event_name = 'CycleConfiguration'
),

offers AS (
    SELECT
        contract_address                                         AS cycle_address,
        lower(decoded_params['nextOffer'])                       AS offer_address,
        toUInt256OrNull(decoded_params['tokenPriceInCRC'])       AS token_price_in_crc_raw,
        toUInt256OrNull(decoded_params['offerLimitInCRC'])       AS offer_limit_in_crc_raw,
        block_timestamp                                          AS offer_created_at,
        concat('0x', transaction_hash)                           AS created_tx_hash
    FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }}
    WHERE event_name = 'NextOfferCreated'
),

deposits AS (
    -- Amount deposited into each offer (supply available to claim).
    SELECT
        contract_address                                         AS cycle_address,
        lower(decoded_params['nextOffer'])                       AS offer_address,
        sum(toFloat64(toUInt256OrZero(decoded_params['amount'])) / 1e18) AS supply_deposited
    FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }}
    WHERE event_name = 'NextOfferTokensDeposited'
    GROUP BY cycle_address, offer_address
),

tokens AS (
    SELECT
        lower(address) AS token_address,
        symbol,
        decimals
    FROM {{ ref('tokens_whitelist') }}
)

SELECT
    o.offer_address                                    AS offer_address,
    o.cycle_address                                    AS cycle_address,
    c.offer_token_address                              AS offer_token_address,
    t.symbol                                           AS offer_token_symbol,
    coalesce(t.decimals, 18)                           AS offer_token_decimals,
    c.admin                                            AS cycle_admin,
    c.cycle_starts_at                                  AS cycle_starts_at,
    c.offer_duration_seconds                           AS offer_duration_seconds,
    o.offer_created_at                                 AS offer_created_at,
    o.created_tx_hash                                  AS created_tx_hash,
    o.token_price_in_crc_raw                           AS token_price_in_crc_raw,
    toFloat64(o.token_price_in_crc_raw) / 1e18         AS token_price_in_crc,
    o.offer_limit_in_crc_raw                           AS offer_limit_in_crc_raw,
    toFloat64(o.offer_limit_in_crc_raw) / 1e18         AS offer_limit_in_crc,
    coalesce(d.supply_deposited, 0.0)                  AS supply_deposited
FROM offers o
INNER JOIN cycles c
    ON c.cycle_address = o.cycle_address
LEFT JOIN tokens t
    ON t.token_address = c.offer_token_address
LEFT JOIN deposits d
    ON d.cycle_address = o.cycle_address
   AND d.offer_address = o.offer_address
