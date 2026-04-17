{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(offer_name)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','marketplace','mart']
  )
}}

WITH payments AS (
    SELECT
        offer_name,
        gateway_address,
        payer,
        block_timestamp
    FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}
)

SELECT
    o.offer_name                                 AS offer_name,
    o.gateway_address                            AS gateway_address,
    o.created_at                                 AS created_at,
    coalesce(count(p.payer), 0)                  AS total_buys,
    coalesce(countDistinct(p.payer), 0)          AS total_payers,
    min(p.block_timestamp)                       AS first_buy_at,
    max(p.block_timestamp)                       AS last_buy_at
FROM {{ ref('int_execution_gnosis_app_marketplace_offers') }} o
LEFT JOIN payments p USING (offer_name)
GROUP BY o.offer_name, o.gateway_address, o.created_at
ORDER BY total_buys DESC
