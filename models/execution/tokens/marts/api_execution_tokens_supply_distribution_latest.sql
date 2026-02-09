{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_supply_distribution', 'granularity:latest']
  )
}}

SELECT
    token_class,
    token,
    value,
    value_usd,
    percentage
FROM {{ ref('fct_execution_tokens_supply_distribution_latest') }}
ORDER BY token_class, value_usd DESC
