{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_supply_total', 'granularity:latest']
  )
}}

SELECT
    value,
    change_pct
FROM {{ ref('fct_execution_stablecoins_overview_latest') }}
WHERE label = 'supply_total'

