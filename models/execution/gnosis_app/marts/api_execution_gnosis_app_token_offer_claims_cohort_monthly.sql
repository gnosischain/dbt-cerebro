{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','token_offers','claims','retention','tier1',
          'api:gnosis_app_token_offer_claims_cohort_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_month", "column": "cohort_month", "operator": ">=",
           "type": "date", "description": "Inclusive start cohort month"},
          {"name": "end_month",   "column": "cohort_month", "operator": "<=",
           "type": "date", "description": "Inclusive end cohort month"}
        ],
        "sort": [{"column": "cohort_month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    retention_pct            AS retention_pct,
    users                    AS value_abs,
    amount_retention_pct     AS amount_retention_pct,
    amount_usd               AS value_usd
FROM {{ ref('fct_execution_gnosis_app_token_offer_claims_cohort_monthly') }}
ORDER BY y, x
