{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','gpay','tier1',
          'api:gnosis_app_gpay_wallets_daily','granularity:daily'],
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

{# Description in schema.yml — see api_execution_gnosis_app_gpay_wallets_daily #}

SELECT
    date,
    onboarding_class,
    n_ga_wallets_new,
    n_ga_wallets_cumulative
FROM {{ ref('fct_execution_gnosis_app_gpay_wallets_daily') }}
ORDER BY date, onboarding_class
