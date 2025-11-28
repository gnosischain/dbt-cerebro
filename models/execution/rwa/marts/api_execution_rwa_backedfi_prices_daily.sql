{{ 
    config(
        materialized='view',
        tags=['production','execution','rwa','backedfi','prices', 'tier1', 'api: backedfi_prices_d']
    )
}}

SELECT
  bticker,
  date,
  price         
FROM {{ ref('fct_execution_rwa_backedfi_prices_daily') }}
ORDER BY
  bticker,
  date
