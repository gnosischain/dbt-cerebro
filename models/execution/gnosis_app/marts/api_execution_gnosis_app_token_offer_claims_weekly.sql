{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','token_offers','claims','tier1',
          'api:gnosis_app_token_offer_claims_weekly','granularity:weekly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "week", "operator": ">=",
           "type": "date", "description": "Inclusive start week"},
          {"name": "end_date",   "column": "week", "operator": "<=",
           "type": "date", "description": "Inclusive end week"}
        ],
        "sort": [{"column": "week", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    week,
    n_claims,
    n_claimers,
    n_offers,
    round(toFloat64(volume_received_token), 6)   AS volume_received_token,
    round(toFloat64(volume_received_usd), 2)     AS volume_received_usd,
    round(toFloat64(volume_spent_crc), 2)        AS volume_spent_crc
FROM {{ ref('fct_execution_gnosis_app_token_offer_claims_weekly') }}
ORDER BY week
