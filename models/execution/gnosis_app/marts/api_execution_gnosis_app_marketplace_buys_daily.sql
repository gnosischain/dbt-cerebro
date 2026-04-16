{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','marketplace','tier1',
          'api:gnosis_app_marketplace_buys_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "offer_name", "column": "offer_name", "operator": "=",
           "type": "string", "description": "Offer name (as declared in createGateway)"},
          {"name": "start_date", "column": "date", "operator": ">=",
           "type": "date", "description": "Inclusive start date"},
          {"name": "end_date",   "column": "date", "operator": "<=",
           "type": "date", "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

{# Description in schema.yml — see api_execution_gnosis_app_marketplace_buys_daily #}

SELECT
    date,
    offer_name,
    n_buys,
    n_payers,
    round(toFloat64(volume_token), 6)  AS volume_token
FROM {{ ref('fct_execution_gnosis_app_marketplace_buys_daily') }}
ORDER BY date, offer_name
