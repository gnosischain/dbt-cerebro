{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app', 'gpay', 'tier1', 'api:gnosis_app_gpay_volume', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "onboarding_class", "column": "onboarding_class", "operator": "=",
           "type": "string", "description": "Onboarding class ('onboarded_via_ga' or 'imported')"},
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
    onboarding_class,
    funded_volume_usd,
    spend_usd,
    spend_count,
    spending_wallets,
    funded_volume_cumulative_usd,
    spend_cumulative_usd
FROM {{ ref('fct_execution_gnosis_app_gpay_volume_daily') }}
WHERE date < today()   -- exclude the current, incomplete day
ORDER BY date, onboarding_class
