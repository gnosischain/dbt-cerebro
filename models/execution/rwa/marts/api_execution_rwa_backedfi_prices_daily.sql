{{ 
    config(
        materialized='view',
        tags=['production','execution', 'tier1', 'api:rwa_backedfi_prices', 'granularity:daily']
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
