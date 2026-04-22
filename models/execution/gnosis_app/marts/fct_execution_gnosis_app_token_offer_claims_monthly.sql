{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(month)',
    tags=['production','execution','gnosis_app','token_offers','claims','mart']
  )
}}

SELECT
    toStartOfMonth(block_timestamp)              AS month,
    count(*)                                     AS n_claims,
    countDistinct(ga_user)                       AS n_claimers,
    countDistinct(offer_address)                 AS n_offers,
    sum(amount_received)                         AS volume_received_token,
    sum(amount_received_usd)                     AS volume_received_usd,
    sum(amount_spent_crc)                        AS volume_spent_crc
FROM {{ ref('int_execution_gnosis_app_token_offer_claims') }}
WHERE toStartOfMonth(block_timestamp) < toStartOfMonth(today())
GROUP BY month
ORDER BY month
