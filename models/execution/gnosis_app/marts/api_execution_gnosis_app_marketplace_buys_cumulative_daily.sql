{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','marketplace','tier1',
          'api:gnosis_app_marketplace_buys_cumulative_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "offer_name", "column": "offer_name", "operator": "=",
           "type": "string", "description": "Offer name"},
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

SELECT
    date,
    offer_name,
    n_buys,
    n_new_payers,
    cumulative_buys,
    cumulative_payers
FROM {{ ref('fct_execution_gnosis_app_marketplace_buys_cumulative_daily') }}
ORDER BY date, offer_name
