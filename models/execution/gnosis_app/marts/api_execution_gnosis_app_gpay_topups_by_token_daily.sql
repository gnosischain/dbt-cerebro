{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','gpay','topups','tier1',
          'api:gnosis_app_gpay_topups_by_token_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "token_bought_symbol", "column": "token_bought_symbol", "operator": "=",
           "type": "string", "description": "Bought token symbol (e.g. 'EURe', 'USDC')"},
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

{# Description in schema.yml — see api_execution_gnosis_app_gpay_topups_by_token_daily #}

SELECT
    date,
    token_bought_symbol,
    n_topups,
    n_ga_users,
    n_gp_wallets,
    round(toFloat64(volume_token_bought), 6)  AS volume_token_bought,
    round(toFloat64(volume_usd), 2)           AS volume_usd
FROM {{ ref('fct_execution_gnosis_app_gpay_topups_by_token_daily') }}
ORDER BY date, token_bought_symbol
