{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_supply_distribution', 'granularity:latest']
  )
}}

SELECT
    token,
    value,
    percentage
FROM {{ ref('fct_execution_stablecoins_supply_distribution_latest') }}
ORDER BY value DESC

