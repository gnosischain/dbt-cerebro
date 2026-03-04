{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_flows_snapshots','granularity:in_ranges'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "symbol",
            "column": "symbol",
            "operator": "=",
            "type": "string",
            "description": "Token symbol"
          },
          {
            "name": "window",
            "column": "window",
            "operator": "=",
            "type": "string",
            "description": "Time window (1D, 7D, 30D, 90D)"
          }
        ]
      }
    }
  )
}}

SELECT
  window
  ,symbol
  ,from_label
  ,to_label
  ,amount_usd
  ,tf_cnt
FROM {{ ref('fct_execution_gpay_flows_snapshot') }}
ORDER BY days ASC 