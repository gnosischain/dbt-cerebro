{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','marketplace','tier1',
          'api:gnosis_app_marketplace_offers_latest','granularity:snapshot'],
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
