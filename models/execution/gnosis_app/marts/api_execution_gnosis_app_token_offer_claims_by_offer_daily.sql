{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','token_offers','claims','tier1',
          'api:gnosis_app_token_offer_claims_by_offer_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "offer_address",   "column": "offer_address",      "operator": "=",
           "type": "string", "description": "Specific offer (nextOffer) address, 0x-prefixed"},
          {"name": "cycle_address",   "column": "cycle_address",      "operator": "=",
           "type": "string", "description": "Specific offer-cycle contract address, 0x-prefixed"},
          {"name": "offer_token_symbol", "column": "offer_token_symbol", "operator": "=",
           "type": "string", "description": "Symbol of the offered token (e.g. 'GNO')"},
          {"name": "start_date",      "column": "date",               "operator": ">=",
           "type": "date",   "description": "Inclusive start date"},
          {"name": "end_date",        "column": "date",               "operator": "<=",
           "type": "date",   "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    date,
    offer_address,
    cycle_address,
    offer_token_symbol,
    n_claims,
    n_claimers,
    round(toFloat64(volume_received_token), 6)  AS volume_received_token,
    round(toFloat64(volume_received_usd), 2)    AS volume_received_usd,
    round(toFloat64(volume_spent_crc), 2)       AS volume_spent_crc,
    round(toFloat64(offer_price_in_crc), 6)     AS offer_price_in_crc
FROM {{ ref('fct_execution_gnosis_app_token_offer_claims_by_offer_daily') }}
ORDER BY date, offer_address
