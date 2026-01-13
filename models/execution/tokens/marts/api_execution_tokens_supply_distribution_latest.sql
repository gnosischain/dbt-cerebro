{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:tokens_supply_distribution', 'granularity:latest']
  )
}}

SELECT
    token_class,
    token,
    value,
    percentage
FROM {{ ref('fct_execution_tokens_supply_distribution_latest') }}
ORDER BY token_class, value DESC
