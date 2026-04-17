{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','gpay','topups','tier1',
          'api:gnosis_app_gpay_topups_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_month", "column": "month", "operator": ">=",
           "type": "date", "description": "Inclusive start month"},
          {"name": "end_month",   "column": "month", "operator": "<=",
           "type": "date", "description": "Inclusive end month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

SELECT * FROM {{ ref('fct_execution_gnosis_app_gpay_topups_monthly') }} ORDER BY month
