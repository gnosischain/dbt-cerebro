{{
  config(
    materialized='view',
    tags=['production','execution','tier0','api:active_senders_per_token', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "token",
            "column": "token",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
          },
          {
            "name": "token_class",
            "column": "token_class",
            "operator": "=",
            "type": "string",
            "description": "Token class (native, stablecoin, bridged, etc.)"
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
  symbol          AS token,
  token_class,
  active_senders  AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
ORDER BY
  date,
  token