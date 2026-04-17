

WITH

create_calls AS (
    SELECT
        transaction_hash,
        block_timestamp,
        decoded_input['name']                    AS offer_name
    FROM `dbt`.`contracts_circles_v2_PaymentGatewayFactory_calls`
    WHERE function_name = 'createGateway'
),

gateway_events AS (
    SELECT
        transaction_hash,
        lower(decoded_params['gateway'])         AS gateway_address,
        lower(decoded_params['owner'])           AS creator
    FROM `dbt`.`contracts_circles_v2_PaymentGatewayFactory_events`
    WHERE event_name = 'GatewayCreated'
),

excluded AS (
    SELECT
        lower(coalesce(offer_name, ''))          AS offer_name_lower,
        lower(coalesce(gateway_address, ''))     AS gateway_address_lower
    FROM `dbt`.`gnosis_app_marketplace_offers_excluded`
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