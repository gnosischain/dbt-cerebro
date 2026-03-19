{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_owner_balances_by_token_daily','granularity:daily'],
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
    date,
    symbol      AS label,
    balance_usd AS value
FROM {{ ref('fct_execution_gpay_owner_balances_by_token_daily') }}
WHERE symbol IN ('EURe', 'GBPe', 'USDC.e')
ORDER BY date, label
