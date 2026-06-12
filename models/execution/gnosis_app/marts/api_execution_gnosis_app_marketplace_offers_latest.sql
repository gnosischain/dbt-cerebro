{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app', 'marketplace', 'tier1', 'api:gnosis_app_marketplace_offers', 'granularity:snapshot'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "offer_name", "column": "offer_name", "operator": "=",
           "type": "string", "description": "Offer name"}
        ],
        "sort": [{"column": "total_buys", "direction": "DESC"}]
      }
    }
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}) AS as_of_date
FROM (
SELECT
    offer_name,
    gateway_address,
    created_at,
    total_buys,
    total_payers,
    first_buy_at,
    last_buy_at
FROM {{ ref('fct_execution_gnosis_app_marketplace_offers_latest') }}
ORDER BY total_buys DESC
) AS sub
