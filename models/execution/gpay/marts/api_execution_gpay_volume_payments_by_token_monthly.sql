{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_volume_payments_by_token_monthly','granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "token",
            "column": "label",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
          },
          {
            "name": "start_date",
            "column": "date",
            "operator": ">=",
            "type": "date",
            "description": "Inclusive start date"
          },
          {
            "name": "end_date",
            "column": "date",
            "operator": "<=",
            "type": "date",
            "description": "Inclusive end date"
          }
        ],
        "sort": [
          {"column": "date", "direction": "DESC"}
        ]
      }
    }
  )
}}

SELECT
    month       AS date,
    token       AS label,
    volume_usd  AS value
FROM {{ ref('fct_execution_gpay_actions_by_token_monthly') }}
WHERE action = 'Payment'
ORDER BY date, label
