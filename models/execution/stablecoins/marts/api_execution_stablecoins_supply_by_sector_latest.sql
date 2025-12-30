{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_supply_by_sector', 'granularity:latest']
  )
}}

SELECT
    sector AS label,
    value,
    value_usd,
    percentage
FROM {{ ref('fct_execution_stablecoins_supply_by_sector_latest') }}
ORDER BY value DESC

