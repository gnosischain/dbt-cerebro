{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','cow','swaps','tier1',
          'api:gnosis_app_swaps_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
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
    n_swaps,
    n_swaps_filled,
    n_swaps_unfilled,
    n_swappers,
    n_orders,
    round(toFloat64(volume_usd_filled), 2)  AS volume_usd_filled,
    round(toFloat64(volume_usd_priced), 2)  AS volume_usd_priced,
    n_filled_unpriced
FROM {{ ref('fct_execution_gnosis_app_swaps_daily') }}
ORDER BY date
