{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','token_offers','claims','tier1',
          'api:gnosis_app_token_offer_claims_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "month", "operator": ">=",
           "type": "date", "description": "Inclusive start month"},
          {"name": "end_date",   "column": "month", "operator": "<=",
           "type": "date", "description": "Inclusive end month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    month,
    n_claims,
    n_claimers,
    n_offers,
    round(toFloat64(volume_received_token), 6)   AS volume_received_token,
    round(toFloat64(volume_received_usd), 2)     AS volume_received_usd,
    round(toFloat64(volume_spent_crc), 2)        AS volume_spent_crc
FROM {{ ref('fct_execution_gnosis_app_token_offer_claims_monthly') }}
ORDER BY month
