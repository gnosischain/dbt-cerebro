{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_payments_by_token', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "token", "column": "label", "operator": "=", "type": "string", "description": "Token symbol"},
          {"name": "start_date", "column": "date", "operator": ">=", "type": "date", "description": "Inclusive start date"},
          {"name": "end_date", "column": "date", "operator": "<=", "type": "date", "description": "Inclusive end date"}
        ],
        "sort": [{"column": "date", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
    date           AS date,
    token          AS label,
    activity_count AS value
FROM {{ ref('fct_celo_gpay_actions_by_token_daily') }}
WHERE action = 'Payment'
ORDER BY date, label
