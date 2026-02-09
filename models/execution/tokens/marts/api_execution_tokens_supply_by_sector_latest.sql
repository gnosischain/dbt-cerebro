{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_supply_by_sector', 'granularity:latest']
  )
}}

SELECT
    token_class,
    sector AS label,
    value,
    value_usd,
    percentage
FROM {{ ref('fct_execution_tokens_supply_by_sector_latest') }}
ORDER BY token_class, value_usd DESC
